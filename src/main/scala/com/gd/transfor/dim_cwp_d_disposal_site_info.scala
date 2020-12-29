package com.gd.transfor

import com.gd.udf.DimCwpDDisposalSiteInfoUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_disposal_site_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_bank_info = MyJDBCUtil.readData(spark, "tbl_dregs_dump_location", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_sys_department = MyJDBCUtil.readData(spark, "sys_department", outProPath)
        val df_tbl_dregs_dump_location = MyJDBCUtil.readData(spark, "tbl_dregs_dump_location", inProPath)
        val df_tbl_consumptivefield_permit = MyJDBCUtil.readData(spark, "tbl_consumptivefield_permit", inProPath)
        val df_mb_dept = MyJDBCUtil.readData(spark, "mb_dept", inProPath)
        val df_tbl_enterprise_info = MyJDBCUtil.readData(spark, "tbl_enterprise_info", inProPath)
        //注册视图
        df_tbl_bank_info.createOrReplaceTempView("tbl_dregs_dump_location")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_sys_department.createOrReplaceTempView("sys_department")
        df_tbl_dregs_dump_location.createOrReplaceTempView("tbl_dregs_dump_location")
        df_tbl_consumptivefield_permit.createOrReplaceTempView("tbl_consumptivefield_permit")
        df_mb_dept.createOrReplaceTempView("mb_dept")
        df_tbl_enterprise_info.createOrReplaceTempView("tbl_enterprise_info")
        //注册udf
        spark.udf.register("addProvince", DimCwpDDisposalSiteInfoUdf.addProvince _)
        spark.udf.register("addCity", DimCwpDDisposalSiteInfoUdf.addCity _)
        spark.udf.register("changeLng", DimCwpDDisposalSiteInfoUdf.changeLng _)
        spark.udf.register("changeLat", DimCwpDDisposalSiteInfoUdf.changeLat _)
        spark.udf.register("changeState", DimCwpDDisposalSiteInfoUdf.changeState _)
        //定义sql(xntype替换为type，增加enable_time、stop_time、enable_userid、stop_userid)
//        val sql1 =
//            """
//              |select disposal_site_id, disposal_site_name, disposal_type,
//              |     c.area_id, disposal_enterprise_id, address, lat,
//              |     lng, radius , area_range, leader_name, leader_phone,
//              |     disposal_picture, c.create_time, c.create_user,
//              |     d.id as update_user, c.update_time, notes,
//              |     addProvince(c.area_id) as province_id, addCity(c.area_id) as city_id,
//              |     c.department_id, audit_state, 2 as dept_id,
//              |     enable_time,stop_time,enable_user,stop_user
//              |from(select a.id as disposal_site_id, a.name as disposal_site_name, type as disposal_type,
//              |         departmentid as area_id, czunitid as disposal_enterprise_id, address, changeLat(longitude, latitude) as lat,
//              |         changeLng(longitude, latitude) as lng, radius ,qyscope as area_range, contact as leader_name, a.phone as leader_phone,
//              |         imgURL as disposal_picture, add_datetime as create_time, b.id as create_user,
//              |         xguserid as update_user, modifytime as update_time, bak as notes, f.id as department_id, changeState(a.state) as audit_state,
//              |         enable_time,stop_time,x.id as enable_user,y.id as stop_user
//              |     from tbl_dregs_dump_location a
//              |         left join sys_user b on a.djuserid=b.username
//              |         left join sys_user x on a.enable_userid=x.username
//              |         left join sys_user y on a.stop_userid=y.username
//              |         left join (select e.id, e.area_id from sys_department e where e.id < 410002 or e.id > 411001 and e.id != 410001016) f on a.departmentid=f.area_id
//              |         ) c
//              |     left join sys_user d on c.update_user=d.username
//              |""".stripMargin

        val sql1 =
            """
              |select disposal_site_id, disposal_site_name, disposal_type,
              |     c.area_id, disposal_enterprise_id, address, lat,
              |     lng, radius , area_range, leader_name, leader_phone,
              |     disposal_picture, c.create_time, c.create_user,
              |     d.id as update_user, c.update_time, notes,
              |     addProvince(c.area_id) as province_id, addCity(c.area_id) as city_id,
              |     c.department_id, audit_state, 2 as dept_id,
              |     enable_time,stop_time,enable_user,stop_user, permit_state, state
              |from (select a.id as disposal_site_id, a.name as disposal_site_name, type as disposal_type,
              |     departmentid as area_id, czunitid as disposal_enterprise_id, address, changeLat(longitude, latitude) as lat,
              |     changeLng(longitude, latitude) as lng, radius ,qyscope as area_range, contact as leader_name, a.phone as leader_phone,
              |     imgURL as disposal_picture, add_datetime as create_time, b.id as create_user,
              |     xguserid as update_user, modifytime as update_time, bak as notes, f.id as department_id, changeState(a.state) as audit_state,
              |     enable_time,stop_time,x.id as enable_user,y.id as stop_user, certicateSate as permit_state, state
              |from (SELECT
              |	temp.*, CASE
              |WHEN temp.xntype = 1 THEN
              |	'消纳处置'
              |WHEN temp.xntype = 2 THEN
              |	'资源化处置(再生企业)'
              |ELSE
              |	''
              |END AS xnTypeName,
              | CASE
              |WHEN temp.state = 0 THEN
              |	'启用'
              |WHEN temp.state = 1 THEN
              |	'停用'
              |WHEN temp.state = 2 THEN
              |	'禁用'
              |ELSE
              |	''
              |END AS stateName,
              | CONCAT(md.`NAME`, m.`NAME`) AS areaName,
              | CASE temp.certicateSate
              |WHEN 0 THEN
              |	'有'
              |WHEN 1 THEN
              |	'无'
              |ELSE
              |	'无'
              |END AS certicateSateName,
              | t.unitname AS czunitname
              |FROM
              |	(
              |		SELECT DISTINCT
              |			s.*, CASE
              |		WHEN 1 = 1 THEN
              |			1
              |		ELSE
              |			1
              |		END AS certicateSate
              |		FROM
              |			tbl_dregs_dump_location s
              |		WHERE
              |			1 = 1
              |		AND s.id NOT IN (
              |			SELECT DISTINCT
              |				s.id
              |			FROM
              |				tbl_dregs_dump_location s,
              |				tbl_consumptivefield_permit p
              |			WHERE
              |				1 = 1
              |			AND s.id = p.disposalsiteid
              |			AND p.state = 0
              |		)
              |		UNION
              |			SELECT DISTINCT
              |				s.*, p.state AS certicateSate
              |			FROM
              |				tbl_dregs_dump_location s,
              |				tbl_consumptivefield_permit p
              |			WHERE
              |				1 = 1
              |			AND s.id = p.disposalsiteid
              |			AND p.state = 0
              |	) temp
              |LEFT JOIN mb_dept m ON temp.departmentid = m.id
              |LEFT JOIN mb_dept md ON m.parent = md.id
              |LEFT JOIN tbl_enterprise_info t ON t.id = temp.czunitid
              |WHERE
              |	1 = 1
              |AND m.parent = '410100'
              |OR m.id = '410100'
              |ORDER BY
              |	temp.modifytime DESC) a
              |     left join sys_user b on a.djuserid=b.username
              |     left join sys_user x on a.enable_userid=x.username
              |     left join sys_user y on a.stop_userid=y.username
              |     left join (select e.id, e.area_id from sys_department e where e.id < 410002 or e.id > 411001 and e.id != 410001016) f on a.departmentid=f.area_id) c
              |         left join sys_user d on c.update_user=d.username
              |""".stripMargin

        var df = spark.sql(sql1)
        df = df.na.fill(410001, Array("department_id"))
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_disposal_site_info=====================ok")
    }

    def getSql = {
        val fields =
            """
              |disposal_site_id,disposal_site_name,disposal_type,area_id,disposal_enterprise_id,
              |address,lat,lng,radius,area_range,leader_name,leader_phone,disposal_picture,create_time,
              |create_user,update_user,update_time,notes,province_id,city_id,department_id,audit_state,dept_id,
              |enable_time,stop_time,enable_user,stop_user,permit_state,state
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_disposal_site_info (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
