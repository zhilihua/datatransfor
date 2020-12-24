package com.gd.transfor

import java.sql.DriverManager
import java.util.Properties

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object sys_user_role {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit = {
        //读取表数据
        val df_sa_user_ext_b2c = MyJDBCUtil.readData(spark, "sa_user_ext_b2c", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)

        //注册成视图
        df_sa_user_ext_b2c.createOrReplaceTempView("sa_user_ext_b2c")
        df_sys_user.createOrReplaceTempView("sys_user")

        //定义sql语句
        val sql1 =
            """
              |select b.id as sys_user_id, MANAGER_GROUP_ID as sys_role_id
              |from sa_user_ext_b2c a
              |     left join sys_user b on a.USER_ID=b.username
              |where b.dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        //落地
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("sys_user_role=============ok")
    }

    def deleteData(proPath: String): Unit = {
        val prop = getProperties(proPath)
        val url = prop.getProperty("mysql.url")
        val username = prop.getProperty("mysql.username")
        val password = prop.getProperty("mysql.password")

        Class.forName("com.mysql.jdbc.Driver")
        val con = DriverManager.getConnection(url, username, password)
        val sql =
            """
              |delete a from sys_user_role a inner join sys_user b on a.sys_user_id = b.id where b.dept_id=2
              |""".stripMargin

        val ps = con.prepareStatement(sql)
        ps.executeUpdate()


        ps.close()
        con.close()
        println("sys_user_role===================clear")
    }
    def getProperties(proPath: String):Properties = {
        val path = MyJDBCUtil.getClass.getClassLoader.getResourceAsStream(proPath)
        val properties: Properties = new Properties()
        properties.load(path)
        properties
    }

    def getSql = {
        val sql =
            """
              |replace into sys_user_role (sys_user_id, sys_role_id)
              |     values(:sys_user_id, :sys_role_id)
              |""".stripMargin

        sql
    }
}
