package com.gd.transfor

import com.gd.udf.OdsCwpVehicleTransportCardUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_vehicle_transport_card {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_vehicletransport_permit = MyJDBCUtil.readData(spark, "tbl_vehicletransport_permit", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_tbl_supervisionunit = MyJDBCUtil.readData(spark, "tbl_supervisionunit", inProPath)
        //注册视图
        df_tbl_vehicletransport_permit.createOrReplaceTempView("tbl_vehicletransport_permit")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_tbl_supervisionunit.createOrReplaceTempView("tbl_supervisionunit")
        //注册udf
        spark.udf.register("changeState", OdsCwpVehicleTransportCardUdf.changeState _)
        //定义sql
        val sql1 =
            """
              |select cid as vehicle_id, ysqyid as transport_enterprise_id, licencekey as vehicle_card_number,
              |     approvalitems as approval_item, c.id as issuance_enterprise_id,
              |     to_date(lssuingdate, "yyyy-MM-dd") as issuance_date,
              |     to_date(startdate, "yyyy-MM-dd") as start_date, to_date(enddate, "yyyy-MM-dd") as end_date,
              |     licencephoto as certification_picture, changeState(a.state) as state,
              |     to_date(cancel_datetime, "yyyy-MM-dd") as invalid_date, reason as invalid_cause, b.id as create_user,
              |     to_date(a.addtime, "yyyy-MM-dd") as create_time,
              |     a.bak as notes, c.id as department_id, 2 as dept_id
              |from tbl_vehicletransport_permit a
              |     left join sys_user b on a.djuserid=b.username
              |     left join tbl_supervisionunit c on a.Issuingunit=c.id where (c.id < 410002 or c.id > 411001) and c.id != 410001016
              |     and b.dept_id=2 and date(a.addtime)>'2019-12-01'
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_vehicle_transport_card"),
            list, outProPath)

        println("ods_cwp_vehicle_transport_card===================ok")
    }
}
