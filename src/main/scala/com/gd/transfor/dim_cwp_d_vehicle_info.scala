package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_vehicle_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_bank_info = MyJDBCUtil.readData(spark, "tbl_vehicleinfo", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_tbl_uservehicle = MyJDBCUtil.readData(spark, "tbl_uservehicle", inProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        val df_dim_cwp_d_terminal_info = MyJDBCUtil.readData(spark, "dim_cwp_d_terminal_info", outProPath)
        val df_tbl_vehicle_driver = MyJDBCUtil.readData(spark, "tbl_vehicle_driver", inProPath)
        val df_dim_cwp_d_driver_info = MyJDBCUtil.readData(spark, "dim_cwp_d_driver_info", outProPath)
        //注册视图
        df_tbl_bank_info.createOrReplaceTempView("tbl_vehicleinfo")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_tbl_uservehicle.createOrReplaceTempView("tbl_uservehicle")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        df_dim_cwp_d_terminal_info.createOrReplaceTempView("dim_cwp_d_terminal_info")
        df_tbl_vehicle_driver.createOrReplaceTempView("tbl_vehicle_driver")
        df_dim_cwp_d_driver_info.createOrReplaceTempView("dim_cwp_d_driver_info")
        //注册udf
        spark.udf.register("changeEnergy", (x: Int) => {
            x match {
                case 0 => 1
                case 1 => 0
                case y => y
            }
        })
        spark.udf.register("addState", (x: Int, y: Any) => {
            var res: String = null
            if(y == 1){
                res=2.toString
            }else if(y == 2) {
                res=3.toString
            }else {
                if(x == 1) res=4.toString
                else res = 1.toString
            }
            res
        })
        //定义sql
        //, GrpID as enterprise_id, DevID as terminal_id    备用  state！=1 and  Flag=0 转为1已审核
        val sql1 =
        """
          |select a.CID as vehicle_id, NO as car_card_number, VIN as frame_number,labelID as label_id,
          |        granttime as label_issuance_date, EngineType as engine_type, Engine as engine_number,
          |        VType as vehicle_model_id, VehicleTypeID as vehicle_type_id, typestate as vehicle_type_state,
          |        ViceEngineType as vice_engine_type_id, ViceEngine as vice_engine_number,
          |        a.Status as vehicle_state, ExtInfo as extend_info1, ExtInfo2 as extend_info2, l.userid,
          |        Mobile as driver_phone, Remark as notes, Flag as flag, ChassisModel as chassis_model,
          |        CarbodyType as car_body_type, CoolerModel as refrigerator_model, HookNum as hook_number,
          |        PurchaseDate as purchase_date,
          |        CarValue as car_original_value, Networth as net_worth, CompDocuments as if_complete,
          |        DeadWeight as vehicle_weight, ApprovedTonnage as approved_tonnage, CarryingTonnage as load_tonnage,
          |        NoLoadTonnage as light_load_tonnage, FullTonnage as heavy_load_tonnage,
          |        OverloadTonnage as over_load_tonnage, Photo as last_picture_id, BodyVolume as car_body_volume,
          |        BodyLen as car_body_length, BodyWidth as car_body_width, BodyHigh as car_body_high,
          |        Brand as car_brand, Factory as factory, LoadingState as load_state, owner, address,
          |        usecharacter as use_property, model as car_model, registerdate as register_time, issuedate as issuance_date,
          |        VehicleLicence as vehicle_card_picture, TeamNumber as vehicle_team_number,
          |        add_datetime as register_date, oil_consumption, gas_consumption, power_comsumption,
          |        water_work_mileage as work_mileage, tanksoil as main_oil_consumption, vicetanksoil as vice_oil_consumption,
          |        color as car_color, syncstatus as synchronous_state, ownerphone as owner_phone,
          |        forcedscrapdate as invalid_date, checkusefuldate as valid_end_date,
          |        b.id as create_user, to_timestamp(add_time, "yyyy-MM-dd HH24:mi:ss") as create_time, c.id as update_user,
          |        to_timestamp(modify_time, "yyyy-MM-dd HH24:mi:ss") as update_time,
          |        changeEnergy(isecocar) as if_new_energy, overallsize as outline_size, inoutflag as change_mode, state,
          |        locked, addState(state,transfer_state) as manage_state, video, d.GrpID as enterprise_id, d.DevID as terminal_id, d.department_id,
          |        1 as audit_state, 2 as dept_id, 0 as is_delete, d.enterprise_name
          |from tbl_vehicleinfo a
          |     left join sys_user b on a.add_man=b.username
          |     left join sys_user c on a.modify_man=c.username
          |     left join (select e.CID, e.devid, e.GrpID, f.department_id, f.enterprise_name from tbl_uservehicle e
          |                     left join dim_cwp_d_enterprise_info f on e.GrpID=f.enterprise_id
          |                     where f.dept_id=2) d on a.CID=d.CID
          |     left join tbl_vehicle_driver l on a.CID=l.CID
          |     where d.enterprise_name is not null
          |""".stripMargin

        var df = spark.sql(sql1)
        //关联设备SN
        df.createOrReplaceTempView("tmp")
        df = spark.sql(
            """
              |select d.*, e.driver_id, e.driver_name from
              |(select a.*, b.terminal_sn, c.id as user_id from tmp a
              |     left join dim_cwp_d_terminal_info b on a.terminal_id=b.terminal_id
              |     left join sys_user c on a.userid=c.username) d
              |     left join dim_cwp_d_driver_info e on d.user_id=e.user_id
              |""".stripMargin)
        df = df.drop("userid").drop("user_id")

        import spark.implicits._
//        df = df.filter($"state" !== 1)
//        df = df.filter($"Flag" === 0)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_vehicle_info================ok")
    }

    def getSql = {
        val fields =
            """
              |vehicle_id,car_card_number,frame_number,label_id,label_issuance_date,engine_type,
              |engine_number,vehicle_model_id,vehicle_type_id,vehicle_type_state,vice_engine_type_id,
              |vice_engine_number,vehicle_state,extend_info1,extend_info2,driver_id,driver_phone,
              |notes,flag,chassis_model,car_body_type,refrigerator_model,hook_number,purchase_date,
              |car_original_value,net_worth,if_complete,vehicle_weight,approved_tonnage,load_tonnage,
              |light_load_tonnage,heavy_load_tonnage,over_load_tonnage,last_picture_id,car_body_volume,
              |car_body_length,car_body_width,car_body_high,car_brand,factory,load_state,owner,address,
              |use_property,car_model,register_time,issuance_date,vehicle_card_picture,vehicle_team_number,
              |register_date,oil_consumption,gas_consumption,power_comsumption,work_mileage,
              |main_oil_consumption,vice_oil_consumption,car_color,synchronous_state,owner_phone,invalid_date,
              |valid_end_date,create_user,create_time,update_user,update_time,if_new_energy,outline_size,
              |change_mode,state,locked,manage_state,video,enterprise_id,terminal_id,department_id,
              |audit_state,dept_id,is_delete,enterprise_name,terminal_sn,driver_name
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_vehicle_info (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
