package com.gd.transfor

import com.gd.udf.OdsCwpCreditIllegalRegistrationUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_credit_illegal_registration {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_credit_illegal_details_interface = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface", inProPath)
        val df_tbl_credit_illegal_details_interface_photo = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface_photo", inProPath)
        val df_tbl_credit_illegal_details_interface_expand = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface_expand", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        val df_dim_cwp_d_driver_info = MyJDBCUtil.readData(spark, "dim_cwp_d_driver_info", outProPath)
        val df_dim_cwp_d_vehicle_info = MyJDBCUtil.readData(spark, "dim_cwp_d_vehicle_info", outProPath)
        //注册视图
        df_tbl_credit_illegal_details_interface.createOrReplaceTempView("tbl_credit_illegal_details_interface")
        df_tbl_credit_illegal_details_interface_photo.createOrReplaceTempView("tbl_credit_illegal_details_interface_photo")
        df_tbl_credit_illegal_details_interface_expand.createOrReplaceTempView("tbl_credit_illegal_details_interface_expand")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        df_dim_cwp_d_driver_info.createOrReplaceTempView("dim_cwp_d_driver_info")
        df_dim_cwp_d_vehicle_info.createOrReplaceTempView("dim_cwp_d_vehicle_info")
        //注册udf
        spark.udf.register("getUrl", OdsCwpCreditIllegalRegistrationUdf.getUrl _)
        spark.udf.register("changeApp", OdsCwpCreditIllegalRegistrationUdf.changeApp _)
        spark.udf.register("getUuid", OdsCwpCreditIllegalRegistrationUdf.getUuid _)
        spark.udf.register("getState", OdsCwpCreditIllegalRegistrationUdf.getState _)
        spark.udf.register("getInt", OdsCwpCreditIllegalRegistrationUdf.getInt _)
        spark.udf.register("changeLng", OdsCwpCreditIllegalRegistrationUdf.SingerNotNullStringChangeLng _)
        spark.udf.register("changeLat", OdsCwpCreditIllegalRegistrationUdf.SingerNotNullStringChangeLat _)
        //定义sql
        val sql1 =
            """
              |select a.id, no as car_card_number, company_name as enterprise_name, a.sn,
              |     driver_name, driver_card as driver_card_number, a.wgbs as illegal_type_code, wgbs_desc as illegal_type_desc,
              |     violation_location as illegal_location, violation_time as illegal_date,
              |     penalty_info as punish_desc,
              |     en_score as enterprise_score, car_driver_score as vehicle_driver_score, en_final_score as enterprise_final_score,
              |     car_driver_final_score as vehicle_driver_final_score, score_year as scoring_year,
              |     d.id as create_user, getInt(supervisionunit_id) as department_id, a.create_time as create_time, a.remarks as notes,
              |     getState(state) as state, changeApp(is_app) as type, enforcer_id, enforcer_name, penalty_amount, c.speed,
              |     getUuid(a.id, is_app) as alarm_uuid, 2 as dept_id, changeLng(c.lng, c.lat) as lng, changeLat(c.lng, c.lat) as lat
              |from tbl_credit_illegal_details_interface a
              |     left join tbl_credit_illegal_details_interface_expand c on a.id=c.credit_illegal_details_interface_id
              |     left join sys_user d on a.user_id=d.username
              |     where a.state != 3 and a.is_app != 1 and a.create_time>'2019-12-01'
              |     and d.dept_id=2
              |""".stripMargin

        var df = spark.sql(sql1)
        df.createOrReplaceTempView("tmp")

        df = spark.sql(
            """
              |select a.*, b.enterprise_id, b.province_id, b.city_id, b.area_id, c.driver_id, d.vehicle_id from tmp a
              |     left join dim_cwp_d_enterprise_info b on a.enterprise_name=b.enterprise_name
              |     left join dim_cwp_d_driver_info c on a.driver_name=c.driver_name
              |     left join dim_cwp_d_vehicle_info d on a.car_card_number=d.car_card_number
              |""".stripMargin)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_credit_illegal_registration"),
            list, outProPath)

        println("ods_cwp_credit_illegal_registration========================ok")
    }
}
