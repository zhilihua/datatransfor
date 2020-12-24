package com.gd.transfor

import com.gd.udf.OdsCwpNoticeInfoUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_notice_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_notice_info = MyJDBCUtil.readData(spark, "tbl_notice_info", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_notice_info.createOrReplaceTempView("tbl_notice_info")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("transferType", OdsCwpNoticeInfoUdf.transferType _)
        //定义sql
        val sql1 =
            """
              |select a.id, publictime as public_time, title, content, noticetype as notice_type, filelink,
              |     transferType(objecttype) as notice_object_type, viewtime as view_time, addtime as create_time,
              |     bak as remark, b.id as create_user,0 as is_delete,2 as dept_id
              |from tbl_notice_info a
              |     left join sys_user b on a.publicaccount = b.username
              |     where b.dept_id=2
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_notice_info"),
            list, outProPath)

        println("ods_cwp_notice_info====================ok")
    }
}
