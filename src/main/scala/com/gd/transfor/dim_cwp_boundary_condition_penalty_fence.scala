package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_boundary_condition_penalty_fence {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_dim_cwp_boundary_condition = MyJDBCUtil.readData(spark, "dim_cwp_boundary_condition", outProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //转换视图
        df_dim_cwp_boundary_condition.createOrReplaceTempView("dim_cwp_boundary_condition")
        df_sys_user.createOrReplaceTempView("sys_user")
        //定义sql
        val sql1 =
            """
              |select id as condition_id, name, comment, operator_account, add_date, 2 as dept_id, 2 as alarm_date, 0 as state
              |from dim_cwp_boundary_condition where dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_boundary_condition_penalty_fence================ok")
    }

    def getSql = {
        val fields =
            """
              |condition_id,name,comment,operator_account,add_date,dept_id,alarm_date,state
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_boundary_condition_penalty_fence (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
