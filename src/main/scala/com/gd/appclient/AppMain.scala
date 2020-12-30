package com.gd.appclient

import com.gd.transfor._
import com.gd.util.{ClearDataUtil, MyJDBCUtil, SqlUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从线上取下来dept_id库数据
 */
object AppMain {
    private val outProPath = "output.properties"
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("dataTransfer")
            .getOrCreate()
        //清理郑州数据
//        ClearDataUtil.run()
        process(spark)
        spark.stop()
    }

    /**
     * 逻辑处理部分
     * @param spark spark驱动
     */
    def process(spark: SparkSession): Unit ={
        //=======================================================================================
        sys_user_role.deleteData(outProPath)
        MyJDBCUtil.delDataList("sys_user", outProPath, "dept_id")

        //1 sys_user
        sys_user.run(spark)

        //用户表中添加admin管理员
        val sys_user_sql = SqlUtil.sys_user_sql
        MyJDBCUtil.updateSingerData(sys_user_sql, outProPath)

        //2 sys_user_role  =============================无dept_id,用sys_dept_id
        sys_user_role.run(spark)

        //========================================================================================
        MyJDBCUtil.delDataList("sys_role", outProPath, "sys_dept_id")
        //3 sys_role
        sys_role.run(spark)

        //添加系统角色
        val sys_role_sql = SqlUtil.sys_role_sql
        MyJDBCUtil.updateSingerData(sys_role_sql, outProPath)

        //添加系统用户菜单
        val sys_role_del_sql = SqlUtil.sys_role_del_sql
        MyJDBCUtil.updateSingerData(sys_role_del_sql, outProPath)
        val sys_role_add_sql = SqlUtil.sys_role_add_sql
        MyJDBCUtil.updateSingerData(sys_role_add_sql, outProPath)

        //4 sys_dept_register  =======================无dept_id，该表丢弃
        //sys_dept_register.run(spark)

        //========================================================================================
        MyJDBCUtil.delDataList("sys_department", outProPath, "dept_id")
        //5 sys_department
        sys_department.run(spark)

        //========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_penalty_fence", outProPath, "dept_id")
        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_work_time", outProPath, "dept_id")
        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_over_speed", outProPath, "dept_id")
        MyJDBCUtil.delDataList("dim_cwp_boundary_condition", outProPath, "dept_id")
        //6 dim_cwp_boundary_condition
        dim_cwp_boundary_condition.run(spark)

        //7 dim_cwp_boundary_condition_penalty_fence
        dim_cwp_boundary_condition_penalty_fence.run(spark)

        //8 dim_cwp_boundary_condition_work_time
        dim_cwp_boundary_condition_work_time.run(spark)

        //9 dim_cwp_boundary_condition_over_speed
        dim_cwp_boundary_condition_over_speed.run(spark)

        //10 dim_cwp_d_area_info ===================无dept_id，该表丢弃
        //dim_cwp_d_area_info.run(spark)

        //========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_info", outProPath, "dept_id")
        //12 dim_cwp_d_enterprise_info
        dim_cwp_d_enterprise_info.run(spark)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_terminal_info", outProPath, "dept_id")
        //13 dim_cwp_d_terminal_info
        dim_cwp_d_terminal_info.run(spark)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_disposal_site_info", outProPath, "dept_id")
        //14 dim_cwp_d_disposal_site_info
        dim_cwp_d_disposal_site_info.run(spark)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_vehicle_info", outProPath, "dept_id")
        //15 dim_cwp_d_vehicle_info
        dim_cwp_d_vehicle_info.run(spark)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_driver_info", outProPath, "dept_id")
        //11 dim_cwp_d_driver_info
        dim_cwp_d_driver_info.run(spark)
        //第二次调用车辆id，添加司机姓名和名称
        dim_cwp_d_vehicle_info.run(spark)

        //根据车辆信息表更新车辆状态表
        val vehicle_state_info_del_sql = SqlUtil.vehicle_state_info_del_sql
        MyJDBCUtil.updateSingerData(vehicle_state_info_del_sql, outProPath)
        val vehicle_state_info_add_sql = SqlUtil.vehicle_state_info_add_sql
        MyJDBCUtil.updateSingerData(vehicle_state_info_add_sql, outProPath)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_business_info", outProPath, "dept_id")
        //16 dim_cwp_d_enterprise_business_info
        dim_cwp_d_enterprise_business_info.run(spark)

        //=========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_bank_account_info", outProPath, "dept_id")
        //17 dim_cwp_d_enterprise_bank_account_info
        dim_cwp_d_enterprise_bank_account_info.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_build_site_info", outProPath, "dept_id")
        //18 dim_cwp_d_build_site_info
        dim_cwp_d_build_site_info.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("dim_cwp_d_site_extension", outProPath, "dept_id")
        //19 dim_cwp_d_site_extension
        dim_cwp_d_site_extension.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_transportation_contract", outProPath, "dept_id")
        //20 ods_cwp_transportation_contract
        ods_cwp_transportation_contract.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_disposal_contract", outProPath, "dept_id")
        //21 ods_cwp_disposal_contract
        ods_cwp_disposal_contract.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_wastedisposal_plan", outProPath, "dept_id")
        //22 ods_cwp_wastedisposal_plan
        ods_cwp_wastedisposal_plan.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_discharge_approval", outProPath, "dept_id")
        //23 ods_cwp_discharge_approval
        ods_cwp_discharge_approval.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_transport_approval", outProPath, "dept_id")
        //24 ods_cwp_transport_approval
        ods_cwp_transport_approval.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_disposal_approval", outProPath, "dept_id")
        //25 ods_cwp_disposal_approval
        ods_cwp_disposal_approval.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_vehicle_service_fee", outProPath, "dept_id")
        //26 ods_cwp_vehicle_service_fee
        ods_cwp_vehicle_service_fee.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_information_manage", outProPath, "dept_id")
        //27 ods_cwp_information_manage
        ods_cwp_information_manage.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_notice_info", outProPath, "dept_id")
        //28 ods_cwp_notice_info
        ods_cwp_notice_info.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_notice_user", outProPath, "dept_id")
        //29 ods_cwp_notice_user
        ods_cwp_notice_user.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_vehicle_transport_card", outProPath, "dept_id")
        //30 ods_cwp_vehicle_transport_card
        ods_cwp_vehicle_transport_card.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card", outProPath, "dept_id")
        //32 ods_cwp_vehicle_register_card
        ods_cwp_vehicle_register_card.run(spark)

        //将车辆状态表中vehicle_type_id的3设置为null
        val vehicle_register_sql = SqlUtil.vehicle_register_sql
        MyJDBCUtil.updateSingerData(vehicle_register_sql, outProPath)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card_apply", outProPath, "dept_id")
        //31 ods_cwp_vehicle_register_card_apply
        ods_cwp_vehicle_register_card_apply.run(spark)

        //将车辆状态申请表中vehicle_type_id的3设置为null
        val vehicle_register_sql_apply = SqlUtil.vehicle_register_sql_apply
        MyJDBCUtil.updateSingerData(vehicle_register_sql_apply, outProPath)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card_state", outProPath, "dept_id")
        //33 ods_cwp_vehicle_register_card_state
        ods_cwp_vehicle_register_card_state.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_order_car", outProPath, "dept_id")
        //34 ods_cwp_order_car
        ods_cwp_order_car.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_send_car", outProPath, "dept_id")
        //35 ods_cwp_send_car
        ods_cwp_send_car.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_credit_illegal_registration", outProPath, "dept_id")
        //36 ods_cwp_credit_illegal_registration
        ods_cwp_credit_illegal_registration.run(spark)

        //==========================================================================================
        MyJDBCUtil.delDataList("ods_cwp_credit_illegal_detail", outProPath, "dept_id")
        //37 ods_cwp_credit_illegal_detail
        ods_cwp_credit_illegal_detail.run(spark)

        //===========================================================================================
        //更新最后信息
        val build_approval_state_sql = SqlUtil.build_approval_state_sql
        MyJDBCUtil.updateSingerData(build_approval_state_sql, outProPath)

    }
}
