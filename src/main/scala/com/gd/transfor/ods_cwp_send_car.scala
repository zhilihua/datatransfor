package com.gd.transfor

import com.gd.udf.OdsCwpSendCarUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_send_car {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_distribution_sign = MyJDBCUtil.readData(spark, "tbl_distribution_sign", inProPath)
        val df_tbl_sendrcar_detail = MyJDBCUtil.readData(spark, "tbl_sendrcar_detail", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_distribution_sign.createOrReplaceTempView("tbl_distribution_sign")
        df_tbl_sendrcar_detail.createOrReplaceTempView("tbl_sendrcar_detail")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("mergeState", OdsCwpSendCarUdf.mergeState _)
        //定义sql
        val sql1 =
            """
              |select a.qyhyid as transportation_contract_id, a.sendcarid as order_car_id, a.cid as vehicle_id,
              |     to_date(startdate, "yyyy-MM-dd") as start_date,
              |     to_date(enddate, "yyyy-MM-dd") as end_date, driver as driver_name,
              |     to_timestamp(ordertime, "yyyy-MM-dd HH:mm:ss") as order_time,
              |     refusalreason as refusal_reason,
              |     to_timestamp(endtime, "yyyy-MM-dd HH:mm:ss") as end_time, reason, mergeState(a.state, b.state) as state,
              |     2 as dept_id
              |from tbl_sendrcar_detail a
              |     left join tbl_distribution_sign b on a.sendcarid=b.sendcarid
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_send_car"),
            list, outProPath)

        println("ods_cwp_send_car==================ok")
    }
}
