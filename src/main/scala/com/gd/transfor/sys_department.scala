package com.gd.transfor

import com.gd.udf.SysDepartmentUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object sys_department {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit = {
        //读取数据
        val df_tbl_supervisionunit = MyJDBCUtil.readData(spark, "tbl_supervisionunit", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册为视图
        df_tbl_supervisionunit.createOrReplaceTempView("tbl_supervisionunit")
        df_sys_user.createOrReplaceTempView("sys_user")
        //定义udf函数
        spark.udf.register("defaultInt", SysDepartmentUdf.defaultInt _)
        spark.udf.register("addPath", SysDepartmentUdf.addPath _)

        //定义sql语句
        val sql1 =
            """
              |select a.id, unitname as name, defaultInt(parentid) as parent_id, lxr as contacts, a.phone, addr as address,
              |     bak as remark, lng as longitude, lat as latitude, deptid as area_id,
              |     to_timestamp(addtime, "yyyy-MM-dd HH24:mi:ss") as create_time,
              |     b.id as create_user, to_timestamp(modifytime, "yyyy-MM-dd HH24:mi:ss") as update_time,
              |     c.id as update_user, defaultInt(state) as is_deleted, addPath(a.id, a.parentid) as path, 2 as dept_id
              |from tbl_supervisionunit a
              |     left join sys_user b on a.djuserid=b.username
              |     left join sys_user c on a.xguserid=c.username
              |     where b.dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("sys_department=================ok")
    }

    def getSql: String = {
        val fields =
            """
              |id,name,parent_id,contacts,phone,address,
              |remark,longitude,latitude,area_id,create_time,
              |create_user,update_time,update_user,is_deleted,path,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into sys_department (${fields})
              |     values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
