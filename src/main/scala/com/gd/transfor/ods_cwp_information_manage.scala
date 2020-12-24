package com.gd.transfor

import com.gd.udf.OdsCwpInformationManageUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_information_manage {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_information_manage = MyJDBCUtil.readData(spark, "tbl_information_manage", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_information_manage.createOrReplaceTempView("tbl_information_manage")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("transferType", OdsCwpInformationManageUdf.transferType _)
        //定义sql
        val sql1 =
            """
              |select a.id, transferType(a.infotype) as info_type, a.title, a.content, a.noticetype as notice_type,
              |     a.filelink, a.addtime as create_time, a.bak as remark, a.state, b.id as create_user,
              |     0 as is_delete, 2 as dept_id
              |from tbl_information_manage a
              |     left join sys_user b on a.publicaccount=b.username
              |     where b.dept_id=2
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_information_manage"),
            list, outProPath)

        println("ods_cwp_information_manage==============ok")
    }
}
