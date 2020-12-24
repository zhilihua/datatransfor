package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_vehicle_service_fee {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_vehicle_service_fee = MyJDBCUtil.readData(spark, "tbl_vehicle_service_fee", inProPath)
        val df_tbl_vehicleinfo = MyJDBCUtil.readData(spark, "tbl_vehicleinfo", inProPath)
        //注册视图
        df_tbl_vehicle_service_fee.createOrReplaceTempView("tbl_vehicle_service_fee")
        df_tbl_vehicleinfo.createOrReplaceTempView("tbl_vehicleinfo")
        //定义sql
        val sql1 =
            """
              |select id, a.cid as vehicle_id, b.no as car_card_number, payment_time, payment_name, service_fee,
              |     start_time, end_time, remarks as notes, feenum, create_time, update_time,
              |     0 as is_delete, 2 as dept_id
              |from tbl_vehicle_service_fee a
              |     left join tbl_vehicleinfo b on a.cid=b.cid
              |""".stripMargin
        import spark.implicits._
        var df = spark.sql(sql1)
        df = df.filter($"car_card_number" !== "")
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_vehicle_service_fee"),
            list, outProPath)

        println("ods_cwp_vehicle_service_fee===================ok")
    }
}
