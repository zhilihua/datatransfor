package com.gd.transfor

import com.gd.udf.SysRoleUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object sys_role {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession): Unit ={
        //读取表数据
        val df_sys_group = MyJDBCUtil.readData(spark, "sys_group", inProPath)

        //注册成视图
        df_sys_group.createOrReplaceTempView("sys_group")

        //定义udf函数
        spark.udf.register("addAuthType", SysRoleUdf.addAuthType _)

        //sql语句
        val sql1 =
            """
              |select GROUP_ID as id, name, GROUP_DESC as remark,
              |     to_timestamp(ADD_DATE, "yyyy-MM-dd HH:mm:ss") as create_time,
              |     to_timestamp(MODIFY_DATE, "yyyy-MM-dd HH:mm:ss") as update_time,
              |     addAuthType(name) as auth_type, 2 as sys_dept_id
              |from sys_group
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)

        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("sys_role=====================ok")
    }

    def getSql = {
        val fields =
            """id,name,remark,create_time,
              |update_time,auth_type,sys_dept_id""".stripMargin
        val sql =
            s"""
              |replace into sys_role (${fields})
              |     values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin

        sql
    }
}
