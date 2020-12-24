package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_wastedisposal_plan {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_wastedisposal_plan = MyJDBCUtil.readData(spark, "tbl_wastedisposal_plan", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_build_site_info = MyJDBCUtil.readData(spark, "dim_cwp_d_build_site_info", outProPath)
        //注册视图
        df_tbl_wastedisposal_plan.createOrReplaceTempView("tbl_wastedisposal_plan")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_build_site_info.createOrReplaceTempView("dim_cwp_d_build_site_info")
        //定义sql
        val sql1 =
            """
              |select a.id, schemename as scheme_name, sgunitid as construction_enterprise_id, jsunitid as build_enterprise_id,
              |     ysunitid as transport_enterprise_id, czunitid as disposal_enterprise_id, buildingsiteid as build_site_id,
              |     engineerproperty as engineer_property, disposalsiteid as disposal_site_id, builtuparea as builtup_area,
              |     totalcost as total_cost, garbagetype as garbage_type, expectpronum as expectpro_num,
              |     disposalbudget as disposal_budget,
              |     to_date(startdate, "yyyy-MM-dd") as start_date, to_date(enddate, "yyyy-MM-dd") as end_date,
              |     backfillnum as back_fillnum, outboundquantity as outbound_quantity, preventemea,
              |     rightsandduties as rightsandduties, precautions as precautions, breresponsibility as breresponsibility,
              |     b.id as create_user, to_timestamp(addtime, "yyyy-MM-dd HH:mm:ss") as create_time, 2 as dept_id,
              |     0 as is_delete, 1 as audit_state, d.department_id
              |from tbl_wastedisposal_plan a
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
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_wastedisposal_plan"),
            list, outProPath)

        println("ods_cwp_wastedisposal_plan================ok")
    }
}
