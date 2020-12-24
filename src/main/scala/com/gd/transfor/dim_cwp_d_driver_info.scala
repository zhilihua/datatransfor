package com.gd.transfor

import com.gd.udf.DimCwpDDriverInfoUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_driver_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_driver_info = MyJDBCUtil.readData(spark, "tbl_driver_info", inProPath)
        val df_member_info = MyJDBCUtil.readData(spark, "member_info", inProPath)
        val df_tbl_vehicle_driver = MyJDBCUtil.readData(spark, "tbl_vehicle_driver", inProPath)
        val df_tbl_uservehicle = MyJDBCUtil.readData(spark, "tbl_uservehicle", inProPath)
        val df_tbl_trancompany_regulatorset = MyJDBCUtil.readData(spark, "tbl_trancompany_regulatorset", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_vehicle_info = MyJDBCUtil.readData(spark, "dim_cwp_d_vehicle_info", outProPath)
        //注册为视图
        df_tbl_driver_info.createOrReplaceTempView("tbl_driver_info")
        df_member_info.createOrReplaceTempView("member_info")
        df_tbl_vehicle_driver.createOrReplaceTempView("tbl_vehicle_driver")
        df_tbl_uservehicle.createOrReplaceTempView("tbl_uservehicle")
        df_tbl_trancompany_regulatorset.createOrReplaceTempView("tbl_trancompany_regulatorset")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_vehicle_info.createOrReplaceTempView("dim_cwp_d_vehicle_info")
        //注册udf函数
        //实现时间加
        spark.udf.register("getEndTime", DimCwpDDriverInfoUdf.getEndTime _)
        //默认补零
        spark.udf.register("defaultInt", DimCwpDDriverInfoUdf.defaultInt _)

        //定义sql
        val sql1 =
            """
              |select driving_license as driver_card_number, b.USER_NAME as driver_name, b.MOBILE as driver_phone,
              |     vehicle_class as allow_driver_car, to_date(valid_from, 'yyyy-MM-dd') as valid_start_date,
              |     to_date(getEndTime(valid_from, valid_for), 'yyyy-MM-dd') as valid_end_date, bak1 as enterprise_id,
              |     bak2 as notes2, bak3 as notes3, h.id as create_user, defaultInt(g.unitid) as department_id,
              |     2 as dept_id, g.CID as vehicle_id, g.userid
              |from tbl_driver_info a
              |     left join member_info b on a.userid=b.user_id
              |     left join sys_user h on a.userid=h.username
              |     left join (
              |         select e.userid, f.unitid, e.CID
              |         from(select c.userid, d.GrpID, c.CID
              |             from tbl_vehicle_driver c
              |                 left join tbl_uservehicle d on c.CID=d.CID) e
              |             left join tbl_trancompany_regulatorset f on e.GrpID=f.tranid
              |     ) g on a.userid=g.userid where h.dept_id=2
              |""".stripMargin
        var df = spark.sql(sql1)
        df.createOrReplaceTempView("tmp")
        df = spark.sql(
            """
              |select a.*, b.enterprise_id, c.id as user_id from tmp a
              |     left join dim_cwp_d_vehicle_info b on a.vehicle_id=b.vehicle_id
              |     left join sys_user c on a.userid=c.username
              |""".stripMargin)
        df = df.drop("vehicle_id").drop("userid")

        import spark.implicits._
        df = df.filter($"department_id" !== -1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_driver_info===============ok")
    }

    def getSql = {
        val fields =
            """
              |driver_card_number,driver_name,driver_phone,allow_driver_car,valid_start_date,
              |valid_end_date,enterprise_id,notes2,notes3,create_user,department_id,dept_id,user_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_driver_info (${fields})
              |values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
