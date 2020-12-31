package com.gd.transfor

import com.gd.udf.OdsCwpVehicleRegisterCardApplyUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_vehicle_register_card_apply {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_twoway_regcard_apply = MyJDBCUtil.readData(spark, "tbl_twoway_regcard_apply", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_build_site_info = MyJDBCUtil.readData(spark, "dim_cwp_d_build_site_info", outProPath)
        val df_ods_cwp_vehicle_register_card = MyJDBCUtil.readData(spark, "ods_cwp_vehicle_register_card", outProPath)
        //注册视图
        df_tbl_twoway_regcard_apply.createOrReplaceTempView("tbl_twoway_regcard_apply")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_build_site_info.createOrReplaceTempView("dim_cwp_d_build_site_info")
        df_ods_cwp_vehicle_register_card.createOrReplaceTempView("ods_cwp_vehicle_register_card")
        //注册udf
        spark.udf.register("addCoords", OdsCwpVehicleRegisterCardApplyUdf.addCoords _)
//        spark.udf.register("changeState", OdsCwpVehicleRegisterCardApplyUdf.changeState _)
        spark.udf.register("changeLines", OdsCwpVehicleRegisterCardApplyUdf.changeLines _)

        val sql1 =
            """
              |select a.id, transportunitid as transport_enterprise_id, builderid as build_enterprise_id,
              |     constructionid as construction_enterprise_id, transid as clean_contract_id,
              |     buildingsiteid as build_site_id, disposaltype as disposal_type,
              |     disposalsiteid as disposal_site_id, disposalunitid as disposal_enterprise_id,
              |     changeLines(drivingline) as plan_line_desc, addCoords(a.lng, a.lat) as coords,
              |     garbagetype as garbage_type,
              |     transportnum as expect_transport_number, to_date(begindate, "yyyy-MM-dd") as start_date,
              |     disposalconid as disposal_contract, taskplan as transport_plan, greenpact as environment_agreement,
              |     b.id as apply_user_account, to_timestamp(applytime, "yyyy-MM-dd HH:mm:ss") as apply_date,
              |     a.state, c.department_id, 2 as dept_id, url as annex_path
              |from tbl_twoway_regcard_apply a
              |     left join sys_user b on a.applyuserid=b.username
              |     left join dim_cwp_d_build_site_info c on a.buildingsiteid=c.build_site_id
              |     where a.state=0 or a.state=1 or a.state=2
              |""".stripMargin
        //where a.applytime>'2019-12-01'
        var df = spark.sql(sql1)
        df.createOrReplaceTempView("tmp")
        df = spark.sql(
            """
              |select a.*, b.is_invol, b.vehicle_type_id from tmp a
              |     left join (
              |     select distinct apply_id, is_invol, vehicle_type_id from ods_cwp_vehicle_register_card) b on a.id=b.apply_id
              |""".stripMargin)

        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_vehicle_register_card_apply"),
            list, outProPath)

        println("ods_cwp_vehicle_register_card_apply=====================ok")
    }
}
