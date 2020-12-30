package com.gd.transfor

import com.gd.udf.DimCwpDBuildSiteInfoUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_build_site_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_bank_info = MyJDBCUtil.readData(spark, "tbl_dregs_source_location", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_sys_department = MyJDBCUtil.readData(spark, "sys_department", outProPath)
        //注册视图
        df_tbl_bank_info.createOrReplaceTempView("tbl_dregs_source_location")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_sys_department.createOrReplaceTempView("sys_department")
        //注册udf
        spark.udf.register("addProvince", DimCwpDBuildSiteInfoUdf.addProvince _)
        spark.udf.register("addCity", DimCwpDBuildSiteInfoUdf.addCity _)
        spark.udf.register("changeLng", DimCwpDBuildSiteInfoUdf.changeLng _)
        spark.udf.register("changeLat", DimCwpDBuildSiteInfoUdf.changeLat _)
        spark.udf.register("changeState", DimCwpDBuildSiteInfoUdf.changeState _)
        spark.udf.register("changeLngLats", DimCwpDBuildSiteInfoUdf.changeLngLat _)
        spark.udf.register("changeType", DimCwpDBuildSiteInfoUdf.changeType _)
        spark.udf.register("changeGrate", (x: Any) => {
            x match {
                case "一类民生工程" => 1
                case "二类民生工程" => 2
                case _ => 3
            }
        })
        //定义sql
        val sql1 =
            """
              |select a.id as build_site_id, name as build_site_name, short_name as build_site_short_name,
              |     changeType(type) as build_site_type, departmentid as area_id, sgunitid as construction_enterprise_id,
              |     jsunitid as build_enterprise_id, address,
              |     changeLng(longitude, latitude) as lng, changeLat(longitude, latitude) as lat, radius,
              |     changeLngLats(qyscope) as area_range, contact as build_leader, a.phone as leader_phone, a.email as leader_email,
              |     imgURL as build_picture, changeState(state) as is_cancelled, bak as notes, b.id as create_user,
              |     add_datetime as create_time, cancel_datetime as cancel_time, modifytime as update_time,
              |     addProvince(b.area_id) as province_id, addCity(b.area_id) as city_id,
              |     d.id as department_id, 0 as is_delete, 2 as dept_id, 1 as audit_state,changeGrate(grade_type) as grade_type
              |from tbl_dregs_source_location a
              |     left join sys_user b on a.djuserid=b.username
              |     left join (select id, area_id from sys_department e where e.id < 410002 or e.id > 411001) d on a.departmentid=d.area_id
              |     where a.departmentid LIKE '4101%'
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_build_site_info===============ok")
    }

    def getSql = {
        val fields =
            """
              |build_site_id,build_site_name,build_site_short_name,build_site_type,area_id,construction_enterprise_id,
              |build_enterprise_id,address,lng,lat,radius,area_range,build_leader,leader_phone,leader_email,
              |build_picture,audit_state,notes,create_user,create_time,cancel_time,update_time,province_id,
              |city_id,department_id,is_delete,dept_id,is_cancelled,grade_type
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_build_site_info(${fields})
              |values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
