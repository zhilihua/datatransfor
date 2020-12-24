package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_notice_user {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_notice_info = MyJDBCUtil.readData(spark, "tbl_notice_info", inProPath)
        //注册为视图
        df_tbl_notice_info.createOrReplaceTempView("tbl_notice_info")

        val sql1 =
            """
              |select id as notice_id, driveruserid as receive_user, 2 as dept_id
              |from tbl_notice_info
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_notice_user"),
            list, outProPath)

        println("ods_cwp_notice_user=======================ok")
    }
}
