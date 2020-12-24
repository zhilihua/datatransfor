package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object sys_dept_register {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit = {
        //读取表数据
        val df_tbl_site_info = MyJDBCUtil.readData(spark, "tbl_site_info", inProPath)
        //注册为视图
        df_tbl_site_info.createOrReplaceTempView("tbl_site_info")
        //sql语句
        val sql1 =
            """
              |select sitename as dept_name, deptid as area_id, gluserid as username,
              |     to_timestamp(addtime, "yyyy-MM-dd HH:mm:ss") as create_time, 1 as audit_state
              |from tbl_site_info
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)
    }

    def getSql = {
        val fields =
            """
              |dept_name,area_id,username,create_time,audit_state
              |""".stripMargin
        val sql =
            s"""
              |replace into sys_dept_register (${fields})
              |     values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
