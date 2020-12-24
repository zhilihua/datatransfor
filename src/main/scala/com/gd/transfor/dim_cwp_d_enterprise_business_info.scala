package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_enterprise_business_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_enterprise_info = MyJDBCUtil.readData(spark, "tbl_business_info", inProPath)
        val df_tbl_trancompany_regulatorset = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_enterprise_info.createOrReplaceTempView("tbl_business_info")
        df_tbl_trancompany_regulatorset.createOrReplaceTempView("sys_user")
        //执行sql语句
        val sql1 =
            """
              |select id, companyid as enterprise_id, scope as business_scope, licensenum as license_number,
              |     thinonepicture as three_one_picture, legalperson as legal_person, IDnumber as legal_person_id,
              |     IDpicture as legal_person_id_picture, lgphone as legal_person_phone, addtime as create_time,
              |     bak as notes, 2 as dept_id
              |from tbl_business_info a
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_enterprise_business_info=================ok")
    }

    def getSql = {
        val fields =
            """
              |id,enterprise_id,business_scope,license_number,three_one_picture,legal_person,
              |legal_person_id,legal_person_id_picture,legal_person_phone,create_time,notes,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_enterprise_business_info (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
