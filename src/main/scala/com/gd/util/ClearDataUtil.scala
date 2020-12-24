package com.gd.util

import com.gd.transfor.sys_user_role

object ClearDataUtil {
    private val outProPath = "output.properties"
    def run(): Unit = {
//        //2 sys_user_role  =============================无dept_id
//        sys_user_role.deleteData(outProPath)
//
//        //1 sys_user =============因为清理sys_user_role时候需要该表
//        MyJDBCUtil.delDataList("sys_user", outProPath, "dept_id")
//
//        //3 sys_role
//        MyJDBCUtil.delDataList("sys_role", outProPath, "sys_dept_id")
//
//        //5 sys_department
//        MyJDBCUtil.delDataList("sys_department", outProPath, "dept_id")
//
//        //7 dim_cwp_boundary_condition_penalty_fence
//        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_penalty_fence", outProPath, "dept_id")
//
//        //8 dim_cwp_boundary_condition_work_time
//        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_work_time", outProPath, "dept_id")
//
//        //9 dim_cwp_boundary_condition_over_speed
//        MyJDBCUtil.delDataList("dim_cwp_boundary_condition_over_speed", outProPath, "dept_id")
//
//        //6 dim_cwp_boundary_condition
//        MyJDBCUtil.delDataList("dim_cwp_boundary_condition", outProPath, "dept_id")
//
//        //12 dim_cwp_d_enterprise_info
//        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_info", outProPath, "dept_id")
//
//        //13 dim_cwp_d_terminal_info
//        MyJDBCUtil.delDataList("dim_cwp_d_terminal_info", outProPath, "dept_id")
//
//        //14 dim_cwp_d_disposal_site_info
//        MyJDBCUtil.delDataList("dim_cwp_d_disposal_site_info", outProPath, "dept_id")
//
//        //15 dim_cwp_d_vehicle_info
//        MyJDBCUtil.delDataList("dim_cwp_d_vehicle_info", outProPath, "dept_id")
//
//        //11 dim_cwp_d_driver_info
//        MyJDBCUtil.delDataList("dim_cwp_d_driver_info", outProPath, "dept_id")
//
//        //16 dim_cwp_d_enterprise_business_info
//        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_business_info", outProPath, "dept_id")
//
//        //17 dim_cwp_d_enterprise_bank_account_info
//        MyJDBCUtil.delDataList("dim_cwp_d_enterprise_bank_account_info", outProPath, "dept_id")
//
//        //18 dim_cwp_d_build_site_info
//        MyJDBCUtil.delDataList("dim_cwp_d_build_site_info", outProPath, "dept_id")
//
//        //19 dim_cwp_d_site_extension
//        MyJDBCUtil.delDataList("dim_cwp_d_site_extension", outProPath, "dept_id")
//
//        //20 ods_cwp_transportation_contract
//        MyJDBCUtil.delDataList("ods_cwp_transportation_contract", outProPath, "dept_id")
//
//        //21 ods_cwp_disposal_contract
//        MyJDBCUtil.delDataList("ods_cwp_disposal_contract", outProPath, "dept_id")
//
//        //22 ods_cwp_wastedisposal_plan
//        MyJDBCUtil.delDataList("ods_cwp_wastedisposal_plan", outProPath, "dept_id")
//
//        //23 ods_cwp_discharge_approval
//        MyJDBCUtil.delDataList("ods_cwp_discharge_approval", outProPath, "dept_id")
//
//        //24 ods_cwp_transport_approval
//        MyJDBCUtil.delDataList("ods_cwp_transport_approval", outProPath, "dept_id")
//
//        //25 ods_cwp_disposal_approval
//        MyJDBCUtil.delDataList("ods_cwp_disposal_approval", outProPath, "dept_id")
//
//        //26 ods_cwp_vehicle_service_fee
//        MyJDBCUtil.delDataList("ods_cwp_vehicle_service_fee", outProPath, "dept_id")
//
//        //27 ods_cwp_information_manage
//        MyJDBCUtil.delDataList("ods_cwp_information_manage", outProPath, "dept_id")
//
//        //28 ods_cwp_notice_info
//        MyJDBCUtil.delDataList("ods_cwp_notice_info", outProPath, "dept_id")
//
//        //29 ods_cwp_notice_user
//        MyJDBCUtil.delDataList("ods_cwp_notice_user", outProPath, "dept_id")
//
//        //30 ods_cwp_vehicle_transport_card
//        MyJDBCUtil.delDataList("ods_cwp_vehicle_transport_card", outProPath, "dept_id")
//
//        //32 ods_cwp_vehicle_register_card
//        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card", outProPath, "dept_id")
//
//        //31 ods_cwp_vehicle_register_card_apply
//        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card_apply", outProPath, "dept_id")
//
//        //33 ods_cwp_vehicle_register_card_state
//        MyJDBCUtil.delDataList("ods_cwp_vehicle_register_card_state", outProPath, "dept_id")
//
//        //34 ods_cwp_order_car
//        MyJDBCUtil.delDataList("ods_cwp_order_car", outProPath, "dept_id")
//
//        //35 ods_cwp_send_car
//        MyJDBCUtil.delDataList("ods_cwp_send_car", outProPath, "dept_id")
//
//        //36 ods_cwp_credit_illegal_registration
//        MyJDBCUtil.delDataList("ods_cwp_credit_illegal_registration", outProPath, "dept_id")
//
//        //37 ods_cwp_credit_illegal_detail
//        MyJDBCUtil.delDataList("ods_cwp_credit_illegal_detail", outProPath, "dept_id")
    }
}
