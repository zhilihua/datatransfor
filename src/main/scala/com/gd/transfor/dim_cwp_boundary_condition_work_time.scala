package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_boundary_condition_work_time {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //读取数据
        val df_tbl_alarm_worktimeset = MyJDBCUtil.readData(spark, "tbl_alarm_worktimeset", inProPath)
        val df_dim_cwp_boundary_condition = MyJDBCUtil.readData(spark, "dim_cwp_boundary_condition", outProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_alarm_worktimeset.createOrReplaceTempView("tbl_alarm_worktimeset")
        df_dim_cwp_boundary_condition.createOrReplaceTempView("dim_cwp_boundary_condition")
        df_sys_user.createOrReplaceTempView("sys_user")
        //定义sql
        val sql1 =
            """
              |select *, 2 as dept_id
              |from(select vehicle_typeID as vehicle_type_id, work_bgeintime as start_date,
              |         work_endtime as end_date, duration_minute as alarm_date,
              |         b.id as operator_account, to_timestamp(add_datetime, "yyyy-MM-dd HH:mm:ss") as add_date
              |     from tbl_alarm_worktimeset a
              |         left join sys_user b on a.operator=b.username where b.dept_id=2) c
              |     cross join (select id as condition_id from dim_cwp_boundary_condition where dept_id=2)
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_boundary_condition_work_time================ok")
    }

    def getSql = {
        val fields =
            """
              |vehicle_type_id,start_date,end_date,alarm_date,operator_account,add_date,condition_id,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_boundary_condition_work_time (${fields})
              |values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
