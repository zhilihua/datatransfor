package com.gd.transfor

import com.gd.udf.DimCwpDEnterpriseInfoUdf
import com.gd.util.{CompanyShortName, DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_enterprise_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"
    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_enterprise_info = MyJDBCUtil.readData(spark, "tbl_enterprise_info", inProPath)
        val df_tbl_trancompany_regulatorset = MyJDBCUtil.readData(spark, "tbl_trancompany_regulatorset", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_sys_department = MyJDBCUtil.readData(spark, "sys_department", outProPath)
        //注册视图
        df_tbl_enterprise_info.createOrReplaceTempView("tbl_enterprise_info")
        df_tbl_trancompany_regulatorset.createOrReplaceTempView("tbl_trancompany_regulatorset")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_sys_department.createOrReplaceTempView("sys_department")
        //注册udf
        spark.udf.register("changeLng", DimCwpDEnterpriseInfoUdf.changeLng _)
        spark.udf.register("changeLat", DimCwpDEnterpriseInfoUdf.changeLat _)
        spark.udf.register("changeState",(x: Int, y: String) => {
            if(x == 3 && y != "410001") 0
            else 1
        })
        spark.udf.register("addShortName", CompanyShortName.getNormalCompanyJcAll _)
        //定义sql
        val sql1 =
            """
              |select a.id as enterprise_id, unitname as enterprise_name, unittype as enterprise_type_id,
              |     c.id as create_user, province as province_id, city as city_id, area as area_id,
              |     addr as address, changeLng(lng, lat) as lng, changeLat(lng, lat) as lat,
              |     officephone as office_phone, lxr as contact_person,
              |     a.phone as contact_phone, unitdesc as enterprise_desc, a.addtime as create_time,
              |     xguserid as update_user, modifytime as update_time, placesnum as vehicle_number,
              |     d.id as department_id, 0 as is_delete, changeState(unittype, f.unitid) as audit_state,
              |     2 as dept_id, addShortName(unitname) as short_name
              |from tbl_enterprise_info a
              |     left join sys_user c on a.gluserid=c.username
              |     left join (select e.id, e.area_id from sys_department e where e.id < 410002 or e.id > 411001) d on a.area=d.area_id
              |     left JOIN (select * from tbl_trancompany_regulatorset where unitid ='410001') f on a.id=f.tranid
              |     where a.city='410100'
              |""".stripMargin

        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_enterprise_info==============ok")
    }

    def getSql = {
        val fields =
            """
              |enterprise_id,enterprise_name,enterprise_type_id,create_user,province_id,city_id,
              |area_id,address,lng,lat,office_phone,contact_person,contact_phone,enterprise_desc,create_time,
              |update_user,update_time,vehicle_number,department_id,is_delete,audit_state,dept_id,short_name
              |""".stripMargin
        val sql =
            s"""
               |replace into dim_cwp_d_enterprise_info (${fields})
               |values(${DfTransferUtil.changeFields(fields)})
               |""".stripMargin

        sql
    }
}
