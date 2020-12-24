package com.gd.transfor

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_order_car {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_ordercar = MyJDBCUtil.readData(spark, "tbl_ordercar", inProPath)
        val df_tbl_sendrcar_detail = MyJDBCUtil.readData(spark, "tbl_sendrcar_detail", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_ordercar.createOrReplaceTempView("tbl_ordercar")
        df_tbl_sendrcar_detail.createOrReplaceTempView("tbl_sendrcar_detail")
        df_sys_user.createOrReplaceTempView("sys_user")
        //定义sql
        val sql1 =
            """
              |select a.id as id, ordercarnum as order_car_no, constructionid as construction_enterprise_id, a.qyhyid as transportation_contract_id,
              |     to_date(a.startdate, "yyyy-MM-dd") as start_date, to_date(a.enddate, "yyyy-MM-dd") as end_date,
              |     carnum as car_num, transcontent as transport_content,
              |     transportnum as transport_num, c.id as create_user,
              |     to_timestamp(a.addtime, "yyyy-MM-dd HH:mm:ss") as create_time, 0 as is_delete,
              |     2 as dept_id
              |from tbl_ordercar a
              |     left join sys_user c on a.aboutcaruser=c.username
              |     where c.dept_id=2
              |""".stripMargin

        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_order_car"),
            list, outProPath)

        println("ods_cwp_order_car======================ok")
    }
}
