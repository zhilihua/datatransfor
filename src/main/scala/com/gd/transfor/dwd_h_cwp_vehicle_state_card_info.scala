package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

/**
 * 更新车辆状态表
 */
object dwd_h_cwp_vehicle_state_card_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit = {
        //获取数据
        val df_dim_cwp_d_vehicle_info = MyJDBCUtil.readData(spark, "dim_cwp_d_vehicle_info", outProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        val df_dim_cwp_d_terminal_info = MyJDBCUtil.readData(spark, "dim_cwp_d_terminal_info", outProPath)
        val df_tbl_posinfo = MyJDBCUtil.readData(spark, "tbl_posinfo", inProPath)
        //注册为视图
        df_dim_cwp_d_vehicle_info.createOrReplaceTempView("dim_cwp_d_vehicle_info")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        df_dim_cwp_d_terminal_info.createOrReplaceTempView("dim_cwp_d_terminal_info")
        df_tbl_posinfo.createOrReplaceTempView("tbl_posinfo")
        //定义udf
        spark.udf.register("changeBool2Int", (x: String) => {
            x match {
                case "false" => 0
                case "true" => 1
            }
        })
        //定义sql
        val sql1 =
            """
              |select a.vehicle_id,a.car_card_number,a.car_brand,a.factory,a.frame_number,a.label_id,a.label_issuance_date,
              |    a.engine_type,a.engine_number,a.vice_engine_type_id,a.vice_engine_number,a.vehicle_model_id,a.vehicle_type_id,
              |    a.vehicle_type_state,a.vehicle_state,a.car_color,a.if_new_energy,a.locked,a.transfer_state,a.video,
              |    a.change_mode,a.invalid_date,a.valid_end_date,a.register_time,a.issuance_date,a.vehicle_card_picture,
              |    a.extend_info1,a.extend_info2,a.driver_id,a.driver_name,a.driver_phone,a.notes,a.outline_size,a.chassis_model,
              |    a.car_body_type,a.refrigerator_model,a.hook_number,a.purchase_date,a.car_original_value,a.net_worth,
              |    a.if_complete,a.vehicle_weight,a.approved_tonnage,a.load_tonnage,a.light_load_tonnage,a.heavy_load_tonnage,
              |    a.over_load_tonnage,a.last_picture_id,a.car_body_volume,a.car_body_length,a.car_body_width,a.car_body_high,
              |    a.load_state,a.owner,a.owner_phone,a.address,a.use_property,a.car_model,a.vehicle_team_number,a.register_date,
              |    a.oil_consumption,a.gas_consumption,a.power_comsumption,a.work_mileage,a.main_oil_consumption,a.vice_oil_consumption,
              |    a.create_user,a.create_time,a.update_user,a.update_time,a.delete_user,a.delete_time,changeBool2Int(a.is_delete) as is_delete,
              |    a.dept_id,
              |    a.department_id,a.enterprise_id,a.terminal_id,a.terminal_sn,a.enterprise_name,a.audit_user,a.audit_time,
              |    a.audit_state,a.manage_state,a.stop_business_state,a.manage_user,a.manage_time,a.manage_reason,b.province_id,
              |    b.city_id,b.area_id,c.enterprise_name as terminal_enterprise_name,d.lng,d.lat,d.time as updatetime
              |    from  dim_cwp_d_vehicle_info a
              |    left join dim_cwp_d_enterprise_info b on a.enterprise_id=b.enterprise_id
              |    left join dim_cwp_d_terminal_info c on a.terminal_sn=c.terminal_sn
              |    left join tbl_posinfo d on a.terminal_id=d.devid
              |    where a.dept_id=2 and a.is_delete=0 and a.audit_state='1' and a.manage_state='1'
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "dwd_h_cwp_vehicle_state_card_info"),
            list, outProPath)

        println("dwd_h_cwp_vehicle_state_card_info===================ok")

    }
}
