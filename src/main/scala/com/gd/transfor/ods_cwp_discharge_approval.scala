package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_discharge_approval {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_discharge_permit = MyJDBCUtil.readData(spark, "tbl_discharge_permit", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_build_site_info = MyJDBCUtil.readData(spark, "dim_cwp_d_build_site_info", outProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        //注册视图
        df_tbl_discharge_permit.createOrReplaceTempView("tbl_discharge_permit")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_build_site_info.createOrReplaceTempView("dim_cwp_d_build_site_info")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        //注册udf
        spark.udf.register("changeState", (x: Int) => {
            x match {
                case 0 => 3
                case 1 => 5
            }
        })
        spark.udf.register("getAEId", (x: String, y: String) => (x, y) match {
            case (a, "") => a
            case ("", b) => b
            case _ => null
        })
        //定义sql
        val sql1 =
            """
              |select a.id, qytype as enterprise_type_id, applyitems as apply_item, buildingsiteid as build_site_id,
              |     number as apply_number, transportunitid as transport_enterprise_id,
              |     legalperson as transport_enterprise_legal, disposalunitid as disposal_enterprise_id,
              |     licencekey as approval_card_number,
              |     to_date(startdate, "yyyy-MM-dd") as start_date, to_date(enddate, "yyyy-MM-dd") as end_date,
              |     licencephoto as approval_card_picture, b.id as issue_user,
              |     to_timestamp(addtime, "yyyy-MM-dd HH:mm:ss") as issue_time, reason as invalid_reason,
              |     to_timestamp(cancel_datetime, "yyyy-MM-dd HH:mm:ss") as invalid_time,
              |     changeState(state) as state, 2 as dept_id, 410100 as area_id,
              |     d.department_id, d.build_site_type,
              |     getAEId(d.construction_enterprise_id, d.build_enterprise_id) as apply_enterprise_id
              |from tbl_discharge_permit a
              |     left join sys_user b on a.djuserid=b.username
              |     left join dim_cwp_d_build_site_info d on a.buildingsiteid=d.build_site_id
              |     where b.dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_discharge_approval"),
            list, outProPath)

        println("ods_cwp_discharge_approval=====================ok")
    }
}
