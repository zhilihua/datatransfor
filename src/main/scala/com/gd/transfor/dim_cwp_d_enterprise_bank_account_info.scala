package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_enterprise_bank_account_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_bank_info = MyJDBCUtil.readData(spark, "tbl_bank_info", inProPath)
        //注册视图
        df_tbl_bank_info.createOrReplaceTempView("tbl_bank_info")
        //定义sql
        val sql1 =
            """
              |select id, dwid as enterprise_id, unitname as enterprise_name, banknum as open_bank_number,
              |     bankname as open_bank_name, account as open_bank_account,
              |     to_timestamp(opendate, "yyyy-MM-dd HH24:mi:ss") as open_bank_date,
              |     bankaddr as open_bank_addr,addtime as create_time, bak as notes,
              |     0 as is_delete,2 as dept_id
              |from tbl_bank_info
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_enterprise_bank_account_info=======================ok")
    }

    def getSql = {
        val fields =
            """
              |id,enterprise_id,enterprise_name,open_bank_number,open_bank_name,open_bank_account,open_bank_date,
              |open_bank_addr,create_time,notes,is_delete,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_enterprise_bank_account_info(${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
