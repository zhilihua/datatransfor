package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_boundary_condition_over_speed {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //读取数据
        val df_tbl_alarm_set = MyJDBCUtil.readData(spark, "tbl_alarm_set", inProPath)
        val df_dim_cwp_boundary_condition = MyJDBCUtil.readData(spark, "dim_cwp_boundary_condition", outProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_alarm_set.createOrReplaceTempView("tbl_alarm_set")
        df_dim_cwp_boundary_condition.createOrReplaceTempView("dim_cwp_boundary_condition")
        df_sys_user.createOrReplaceTempView("sys_user")
        //定义sql
        val sql1 =
            """
              |select *, 2 as dept_id, 60 as speed, '四环内速度' as name
              |from(select duration_minute as alarm_date, b.id as operator_account,
              |         to_timestamp(add_datetime, "yyyy-MM-dd HH:mm:ss") as add_date
              |     from tbl_alarm_set a
              |         left join sys_user b on a.operator=b.username where b.dept_id=2)
              |     cross join (select id as condition_id from dim_cwp_boundary_condition where dept_id=2)
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_boundary_condition_over_speed============ok")
    }

    def getSql = {
        val fields =
            """
              |alarm_date,operator_account,add_date,condition_id,dept_id,speed,name
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_boundary_condition_over_speed (${fields})
              |values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
