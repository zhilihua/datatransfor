package com.gd.util

object SqlUtil {
    //添加系统用户
    val sys_user_sql = "UPDATE sys_user SET auth_type=1, phone='18211111111' where username='admin'"

    //添加系统角色
     val sys_role_sql = """INSERT INTO sys_role
    (id, name, remark, sys_dept_id, sys_dept_name, create_time, create_user, update_time, update_user, delete_time, delete_user, is_deleted, auth_type, app_modules)
    VALUES(999, '系统管理员', '系统管理员，拥有最高权限', 2, '郑州市城管局', '2020-01-08 00:21:14.000', 1, '2020-07-02 00:02:29.000', 1, NULL, NULL, 0, '1',
    'carSearch,carDynamic,carAudit,checkScoring,sitePatrol,siteProspect,checkAudit,checkSure,givenSiteMonitoring,givenBackfill,illegalAlarmStatistics')"""

    //删除郑州对应的菜单
     val sys_role_del_sql = "DELETE FROM sys_role_menu WHERE sys_role_id =999"

    //角色对应的菜单
     val sys_role_add_sql = "INSERT INTO sys_role_menu (sys_role_id, sys_user_menu_id) select 999, id from sys_menu where auth_type!='2' and auth_type!='3'"

    //删除车辆状态表中的郑州车辆
     val vehicle_state_info_del_sql = "DELETE FROM dwd_h_cwp_vehicle_state_card_info WHERE dept_id=2"

    //更新车辆状态
    val vehicle_state_info_add_sql = """insert into dwd_h_cwp_vehicle_state_card_info (
        vehicle_id,car_card_number,car_brand,factory,frame_number,label_id,label_issuance_date,engine_type,
        engine_number,vice_engine_type_id,vice_engine_number,vehicle_model_id,vehicle_type_id,vehicle_type_state,
        vehicle_state,car_color,if_new_energy,locked,transfer_state,video,change_mode,invalid_date,valid_end_date,
        register_time,issuance_date,vehicle_card_picture,extend_info1,extend_info2,driver_id,driver_name,
        driver_phone,notes,outline_size,chassis_model,car_body_type,refrigerator_model,hook_number,purchase_date,
        car_original_value,net_worth,if_complete,vehicle_weight,approved_tonnage,load_tonnage,light_load_tonnage,
        heavy_load_tonnage,over_load_tonnage,last_picture_id,car_body_volume,car_body_length,car_body_width,
        car_body_high,load_state,owner,owner_phone,address,use_property,car_model,vehicle_team_number,register_date,
        oil_consumption,gas_consumption,power_comsumption,work_mileage,main_oil_consumption,vice_oil_consumption,
        create_user,create_time,update_user,update_time,delete_user,delete_time,is_delete,dept_id,department_id,
        enterprise_id,terminal_id,terminal_sn,enterprise_name,audit_user,audit_time,audit_state,manage_state,
        stop_business_state,manage_user,manage_time,manage_reason,province_id,city_id,area_id,terminal_enterprise_name
    ) select a.vehicle_id,
    a.car_card_number,
    a.car_brand,
    a.factory,
    a.frame_number,
    a.label_id,
    a.label_issuance_date,
    a.engine_type,
    a.engine_number,
    a.vice_engine_type_id,
    a.vice_engine_number,
    a.vehicle_model_id,
    a.vehicle_type_id,
    a.vehicle_type_state,
    a.vehicle_state,
    a.car_color,
    a.if_new_energy,
    a.locked,
    a.transfer_state,
    a.video,
    a.change_mode,
    a.invalid_date,
    a.valid_end_date,
    a.register_time,
    a.issuance_date,
    a.vehicle_card_picture,
    a.extend_info1,
    a.extend_info2,
    a.driver_id,
    a.driver_name,
    a.driver_phone,
    a.notes,
    a.outline_size,
    a.chassis_model,
    a.car_body_type,
    a.refrigerator_model,
    a.hook_number,
    a.purchase_date,
    a.car_original_value,
    a.net_worth,
    a.if_complete,
    a.vehicle_weight,
    a.approved_tonnage,
    a.load_tonnage,
    a.light_load_tonnage,
    a.heavy_load_tonnage,
    a.over_load_tonnage,
    a.last_picture_id,
    a.car_body_volume,
    a.car_body_length,
    a.car_body_width,
    a.car_body_high,
    a.load_state,
    a.owner,
    a.owner_phone,
    a.address,
    a.use_property,
    a.car_model,
    a.vehicle_team_number,
    a.register_date,
    a.oil_consumption,
    a.gas_consumption,
    a.power_comsumption,
    a.work_mileage,
    a.main_oil_consumption,
    a.vice_oil_consumption,
    a.create_user,
    a.create_time,
    a.update_user,
    a.update_time,
    a.delete_user,
    a.delete_time,
    a.is_delete,
    a.dept_id,
    a.department_id,
    a.enterprise_id,
    a.terminal_id,
    a.terminal_sn,
    a.enterprise_name,
    a.audit_user,
    a.audit_time,
    a.audit_state,
    a.manage_state,
    a.stop_business_state,
    a.manage_user,
    a.manage_time,
    a.manage_reason,
    b.province_id,
    b.city_id,
    b.area_id,
    c.enterprise_name
    from  dim_cwp_d_vehicle_info a
    left join dim_cwp_d_enterprise_info b on a.enterprise_id=b.enterprise_id
    left join dim_cwp_d_terminal_info c on a.terminal_sn=c.terminal_sn
    where a.dept_id=2 and a.is_delete=0 and a.audit_state='1'"""

    //将车辆状态表中vehicle_type_id的3设置为null
    val vehicle_register_sql = "UPDATE ods_cwp_vehicle_register_card SET vehicle_type_id = null where vehicle_type_id=3"

    //将车辆状态申请表中vehicle_type_id的3设置为null
    val vehicle_register_sql_apply = "UPDATE ods_cwp_vehicle_register_card_apply SET vehicle_type_id = null where vehicle_type_id=3"
}
