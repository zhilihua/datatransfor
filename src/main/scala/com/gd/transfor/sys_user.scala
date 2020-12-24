package com.gd.transfor

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.gd.udf.SysUserUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object sys_user {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit = {
        //读取表数据
        val df_user = MyJDBCUtil.readData(spark, "sa_user", inProPath)
        val df_member = MyJDBCUtil.readData(spark, "member_info", inProPath)
        val df_enterprise = MyJDBCUtil.readData(spark, "tbl_enterprise_info", inProPath)
        val df_supervisionunit = MyJDBCUtil.readData(spark, "tbl_supervisionunit", inProPath)

        //注册为视图
        df_user.createOrReplaceTempView("sa_user")
        df_member.createOrReplaceTempView("member_info")
        df_enterprise.createOrReplaceTempView("tbl_enterprise_info")
        df_supervisionunit.createOrReplaceTempView("tbl_supervisionunit")

        //定义udf函数
        spark.udf.register("getEnterpriseId", SysUserUdf.getEnterpriseId _)
        spark.udf.register("getDepartmentId", SysUserUdf.getDepartmentId _)
        spark.udf.register("parserPWD", SysUserUdf.gdMD5 _)
        spark.udf.register("to_string", (x: Double) => {x.toString})
        spark.udf.register("changeStatus", SysUserUdf.changeStatus _)
        spark.udf.register("addDepartment", SysUserUdf.addDepartment _)
        spark.udf.register("addEnterpriseName", SysUserUdf.addEnterpriseName _)

        val now: String = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

        //将多个表关联到一起
        val sql1 =
            s"""
              |select c.username, c.real_name, c.status, c.create_time, c.email,
              |     c.phone, c.enterprise_id, addDepartment(c.enterprise_type_id) as department,
              |     salt_code, department_id,  md5(CONCAT(c.password, salt_code)) as password,
              |     0 as disabled, 2 as dept_id, 0 as auth_type,
              |     addEnterpriseName(d.unitname, e.unitname) as enterprise_name,
              |     addEnterpriseName(d.area, e.deptid) as area_id, '$now' as last_login_time
              |from(select a.user_id as username, parserPWD(pwd) as password, user_name as real_name, changeStatus(state) as status,
              |     join_date as create_time, msn as email, mobile as phone, getEnterpriseId(EXT_INT_9, EXT_STRING_9) as enterprise_id,
              |     EXT_INT_9 as enterprise_type_id, getDepartmentId(EXT_INT_9, EXT_STRING_9) as department_id,
              |     substring(MD5(to_string(RAND())),1,6) as salt_code
              |     from member_info a
              |         join sa_user b on a.user_id = b.user_id) c
              |     left join tbl_enterprise_info d on c.enterprise_id=d.id
              |     left join tbl_supervisionunit e on c.enterprise_id=e.id
              |""".stripMargin
        val df = spark.sql(sql1)

        //填充数据
        val newDF = df.na.fill("410100", Array("area_id"))
//        newDF.show()

        val list = DfTransferUtil.df2Map(newDF)

        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("sys_user=============ok")
    }

    def getSql: String = {

        val sql =
            s"""
              |replace into sys_user (username,real_name,status,create_time,email,phone,enterprise_id,department,salt_code,
              |    department_id,password,disabled,dept_id,auth_type,enterprise_name,area_id,last_login_time)
              |    values(:username,:real_name,:status,:create_time,:email,:phone,:enterprise_id,:department,:salt_code,
              |         :department_id,:password,:disabled,:dept_id,:auth_type,:enterprise_name,:area_id,:last_login_time)
              |""".stripMargin

        sql
    }
}
