package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object dim_cwp_d_area_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_mb_dept = MyJDBCUtil.readData(spark, "mb_dept", inProPath)
        //注册成视图
        df_mb_dept.createOrReplaceTempView("mb_dept")
        //定义sql
        val sql1 =
            """
              |select id, NAME as area_name, parent as parent_id, fzr as leader_name, lxr as contact_person,
              |     phone as contact_phone, addr as address, llevel as level, memo as desc, priority as sort,
              |     active as area_if_valid, Longitude as lng, Latitude as lat,MocaLongitude as moca_lng,
              |     MocaLatitude as moca_lat, userid as registrant, ts as register_date, districtCode as area_code
              |from mb_dept
              |""".stripMargin
        val df = spark.sql(sql1)
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)
    }

    def getSql = {
        val fields =
            """
              |id,area_name,parent_id,leader_name,contact_person,contact_phone,
              |address,`level`,`desc`,`sort`,area_if_valid,lng,lat,moca_lng,moca_lat,registrant,register_date,area_code
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_area_info (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
