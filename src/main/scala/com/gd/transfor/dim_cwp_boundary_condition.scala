package com.gd.transfor

import com.gd.udf.DimCwpBoundaryConditionUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_boundary_condition {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //读取数据
        val df_tbl_alarm_penaltyzone = MyJDBCUtil.readData(spark, "tbl_alarm_penaltyzone", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //创建视图
        df_tbl_alarm_penaltyzone.createOrReplaceTempView("tbl_alarm_penaltyzone")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("getCoords", DimCwpBoundaryConditionUdf.addCoords _)
        spark.udf.register("changeLng", DimCwpBoundaryConditionUdf.changeLng _)
        spark.udf.register("changeLat", DimCwpBoundaryConditionUdf.changeLat _)
        //定义sql
        val sql1 =
            """
              |select reservename as name, rangedesc as comment, enclosureid as area_id,
              |     getCoords(changeLng(Lng, Lat), changeLat(Lng, Lat)) as coords,
              |     Rad as radius, b.id as operator_account, to_timestamp(add_datetime, "yyyy-MM-dd HH:mm:ss") as add_date,
              |     2 as dept_id
              |from tbl_alarm_penaltyzone a
              |     left join sys_user b on a.operator = b.username
              |where b.dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_boundary_condition=================ok")
    }

    def getSql = {
        val fields =
            """
              |name,comment,area_id,coords,radius,operator_account,add_date,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_boundary_condition (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
