package com.gd.transfor

import java.text.SimpleDateFormat

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_vehicle_register_card_state {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_twoway_regcard_applydetail = MyJDBCUtil.readData(spark, "tbl_twoway_regcard_applydetail", inProPath)
        val df_ods_cwp_vehicle_register_card_apply = MyJDBCUtil.readData(spark, "ods_cwp_vehicle_register_card_apply", outProPath)
        val df_tbl_twoway_reg_card = MyJDBCUtil.readData(spark, "tbl_twoway_reg_card", inProPath)
        //注册视图
        df_tbl_twoway_regcard_applydetail.createOrReplaceTempView("tbl_twoway_regcard_applydetail")
        df_ods_cwp_vehicle_register_card_apply.createOrReplaceTempView("ods_cwp_vehicle_register_card_apply")
        df_tbl_twoway_reg_card.createOrReplaceTempView("tbl_twoway_reg_card")
        spark.udf.register("changeState", (x: Int) => {
            x match {
                case 0 => 1
                case 1 => 3
            }
        })
        //定义sql
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//        val sql1 =
//            """
//              |select a.id, applyid as apply_id, cid as vehicle_id, changeState(c.state) as state, reason as return_cause, 2 as dept_id
//              |from tbl_twoway_regcard_applydetail a
//              |     inner join ods_cwp_vehicle_register_card_apply b on a.applyid=b.id
//              |     left join ods_cwp_vehicle_register_card c on a.cid=c.vehicle_id and a.applyid=c.apply_id
//              |""".stripMargin

        val sql1 =
            """
              |select c.* from
              |(select a.applyid as apply_id,a.cid as vehicle_id,
              |case when b.state = 1 then 3
              |     when b.cid is null then 0 else 1 end state, a.id, a.reason as return_cause, 2 as dept_id
              |from tbl_twoway_regcard_applydetail a left JOIN tbl_twoway_reg_card b
              |     on a.applyid = b.applyid and a.cid = b.cid where a.state in ('0','1')) c
              |     inner join ods_cwp_vehicle_register_card_apply d on c.apply_id=d.id
              |""".stripMargin

        var df = spark.sql(sql1)
        import spark.implicits._

        df = df.filter($"state" !==2)
//        df.createOrReplaceTempView("temp")

//        val sql2 =
//            """select apply_id, vehicle_id, state, return_cause, dept_id
//              |from temp where id in
//              |(select max(id) from temp group by apply_id, vehicle_id)
//              |""".stripMargin
//        val newDF = spark.sql(sql2)
        val newDF = df.drop("id")

        var columns = ""
        for(col <- newDF.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(newDF)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_vehicle_register_card_state"),
            list, outProPath)

        println("ods_cwp_vehicle_register_card_state=========================ok")
    }
}
