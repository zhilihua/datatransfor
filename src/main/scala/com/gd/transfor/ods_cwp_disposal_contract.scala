package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_disposal_contract {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_disposal_contract = MyJDBCUtil.readData(spark, "tbl_disposal_contract", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_build_site_info = MyJDBCUtil.readData(spark, "dim_cwp_d_build_site_info", outProPath)
        //注册视图
        df_tbl_disposal_contract.createOrReplaceTempView("tbl_disposal_contract")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_build_site_info.createOrReplaceTempView("dim_cwp_d_build_site_info")
        //定义udf函数
        spark.udf.register("changeState", (x: String) => {
            var result = ""
            val strings = x.split(",")
            for (str <- strings){
                val value = str.toInt match {
                    case 5 => 1
                    case 6 => 2
                    case 7 => 3
                    case 8 => 4
                    case 9 => 5
                    case 10 => 6
                    case _ => ""
                }
                result += value.toString+","
            }
            result.substring(0, result.length-1)
        })
        //定义sql
        val sql1 =
            """
              |select a.id, contractnumber as contract_number, contractname as contract_name, contractpartyA as contract_party_type,
              |     constructionid as construction_enterprise_id, buildid as build_enterprise_id,
              |     buildingsiteid as build_site_id, transportunitid as transport_enterprise_id,
              |     disposalsitetype as disposal_site_type, disposalsiteid as disposal_site_id,
              |     disposalunitid as disposal_enterprise_id, changeState(garbagetype) as garbage_type,
              |     disposalnum as disposal_num, disposalunit as disposal_unit,
              |     to_date(disposalstartdate, "yyyy-MM-dd") as disposal_start_date,
              |     to_date(disposalenddate, "yyyy-MM-dd") as disposal_end_date, disposalprice as disposal_unit_price,
              |     totalprice as total_price, settlementmethod as settlement_method, defaultclause as default_clause,
              |     to_date(startdate, "yyyy-MM-dd") as start_date, to_date(enddate, "yyyy-MM-dd") as end_date,
              |     contractphoto as contract_photo, b.id as create_user,
              |     to_timestamp(addtime, "yyyy-MM-dd HH:mm:ss") as create_time, bak as remark, c.id as audit_user,
              |     to_timestamp(confirmtime, "yyyy-MM-dd HH:mm:ss") as audit_time, state as audit_state, 2 as dept_id,
              |     d.department_id
              |from tbl_disposal_contract a
              |     left join sys_user b on a.djuserid=b.username
              |     left join sys_user c on a.qruserid=c.username
              |     left join dim_cwp_d_build_site_info d on a.buildingsiteid=d.build_site_id
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_disposal_contract"),
            list, outProPath)

        println("ods_cwp_disposal_contract===================ok")
    }
}
