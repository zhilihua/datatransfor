package com.gd.transfor

import com.gd.udf.OdsCwpCreditIllegalDetailUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import scala.collection.mutable.ArrayBuffer

object ods_cwp_credit_illegal_detail {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_credit_illegal_details_interface = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface", inProPath)
        val df_tbl_credit_illegal_details_interface_photo = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface_photo", inProPath)
        val df_tbl_credit_illegal_details_interface_expand = MyJDBCUtil.readData(spark, "tbl_credit_illegal_details_interface_expand", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        val df_dim_cwp_d_driver_info = MyJDBCUtil.readData(spark, "dim_cwp_d_driver_info", outProPath)
        val df_dim_cwp_d_vehicle_info = MyJDBCUtil.readData(spark, "dim_cwp_d_vehicle_info", outProPath)
        //注册视图
        df_tbl_credit_illegal_details_interface.createOrReplaceTempView("tbl_credit_illegal_details_interface")
        df_tbl_credit_illegal_details_interface_photo.createOrReplaceTempView("tbl_credit_illegal_details_interface_photo")
        df_tbl_credit_illegal_details_interface_expand.createOrReplaceTempView("tbl_credit_illegal_details_interface_expand")
        df_sys_user.createOrReplaceTempView("sys_user")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        df_dim_cwp_d_driver_info.createOrReplaceTempView("dim_cwp_d_driver_info")
        df_dim_cwp_d_vehicle_info.createOrReplaceTempView("dim_cwp_d_vehicle_info")
        //注册udf
        spark.udf.register("getUrl", (x: String, y: Int, flag: String) => {
            var res: String = null
            if(flag == "vehicle"){
                if(y == 2)  res = x
            }
            else if(flag == "driving"){
                if(y == 1) res = x
            }
            else if(flag == "scene") {
                if(y == 3) res = x
            }
            res
        })
        spark.udf.register("changeApp", (x: Any)=>{
            var res = "-"
            if(x==0||x==3) res = 0.toString
            else if(x==2) res = 1.toString
            res
        })
        spark.udf.register("getUuid", (x: Int, y:Int) => {
            var res: String = null
            if(y == 2) res = x.toString
            res
        })
        spark.udf.register("getState", (x:Int)=>{
            var res = x
            if(x==1) res = 3
            else if(x==2) res = 6
            else if(x == 4) res = 8
            res
        })
        spark.udf.register("getInt", (x: Int) => {x})
        spark.udf.register("changeLng", OdsCwpCreditIllegalDetailUdf.SingerNotNullStringChangeLng _)
        spark.udf.register("changeLat", OdsCwpCreditIllegalDetailUdf.SingerNotNullStringChangeLat _)

        //定义sql
        val sql1 =
            """
              |select a.id, cid as vehicle_id, no as car_card_number, company_name as enterprise_name, a.dept_id as city_id,
              |     driver_name, driver_card as driver_card_number, wgbs_desc as illegal_type_desc,
              |     violation_location as illegal_location,
              |     to_timestamp(violation_time, "yyyy-MM-dd HH:mm:ss") as illegal_date,
              |     getUrl(b.photo_url, b.type, "vehicle") as vehicle_licence_image_url,
              |     getUrl(b.photo_url, b.type, "driving") as driving_licence_image_url,
              |     getUrl(b.photo_url, b.type, "scene") as scene_image_url, penalty_info as punish_desc,
              |     en_score as enterprise_score, car_driver_score as vehicle_driver_score, en_final_score as enterprise_final_score,
              |     car_driver_final_score as vehicle_driver_final_score, score_year as scoring_year,
              |     d.id as create_user, getInt(supervisionunit_id) as department_id,
              |     to_timestamp(a.create_time, "yyyy-MM-dd HH:mm:ss") as create_time, a.remarks as notes,
              |     getState(state) as state, changeApp(is_app) as type, enforcer_id, enforcer_name, penalty_amount, c.speed,
              |     getUuid(a.id, is_app) as alarm_uuid, 2 as dept_id, changeLng(c.lng, c.lat) as lng, changeLat(c.lng, c.lat) as lat,
              |     e.enterprise_id, a.wgbs as illegal_type_code
              |from tbl_credit_illegal_details_interface a
              |     left join tbl_credit_illegal_details_interface_photo b on a.id=b.credit_illegal_details_interface_id
              |     left join tbl_credit_illegal_details_interface_expand c on a.id=c.credit_illegal_details_interface_id
              |     left join sys_user d on a.user_id=d.username
              |     left join dim_cwp_d_enterprise_info e on a.company_name=e.enterprise_name
              |     where (a.state = 2 or a.state = 7)
              |""".stripMargin

//        and a.create_time>'2019-12-01' and and e.enterprise_id is not null
        val df = spark.sql(sql1)

        //进行聚合
        val rdd = df.rdd

        val rdd1 = rdd.map(
            line => (line(0), line)  //将设备id作为键，整体作为值，然后一个个过滤
        ).groupByKey()


        val rdd2 = rdd1.map(data => {
            var res: Row = null
            val mid = data._2.toList

            if(mid.length <= 1) res = mid.head
            else {
                val v_url = new ArrayBuffer[String]
                val d_url = new ArrayBuffer[String]
                val s_url = new ArrayBuffer[String]

                for(i <- mid.indices){
                    if(mid(i)(10) != "" && mid(i)(10) != null ) v_url.append(mid(i)(10).toString)
                    if(mid(i)(11) != "" && mid(i)(11) != null ) d_url.append(mid(i)(11).toString)
                    if(mid(i)(12) != "" && mid(i)(12) != null ) s_url.append(mid(i)(12).toString)
                }

                res = Row(mid(0)(0), mid(0)(1), mid(0)(2), mid(0)(3), mid(0)(4), mid(0)(5),
                    mid(0)(6), mid(0)(7), mid(0)(8),mid(0)(9), v_url.mkString(","), d_url.mkString(","),
                    s_url.mkString(","), mid(0)(13), mid(0)(14), mid(0)(15), mid(0)(16), mid(0)(17),
                    mid(0)(18), mid(0)(19), mid(0)(20), mid(0)(21), mid(0)(22), mid(0)(23),
                    mid(0)(24), mid(0)(25), mid(0)(26), mid(0)(27), mid(0)(28), mid(0)(29),
                    mid(0)(30),mid(0)(31),mid(0)(32),mid(0)(33),mid(0)(34)
                )
            }

            res
        })

        //转换为DF
        val schemaString = "id,vehicle_id,car_card_number,enterprise_name,city_id,driver_name," +
            "driver_card_number,illegal_type_desc,illegal_location,illegal_date,vehicle_licence_image_url,driving_licence_image_url,"+
            "scene_image_url,punish_desc,enterprise_score,vehicle_driver_score,enterprise_final_score,vehicle_driver_final_score,"+
            "scoring_year,create_user,department_id,create_time,notes,state," +
            "type,enforcer_id,enforcer_name,penalty_amount,speed,alarm_uuid," +
            "dept_id,lng,lat,enterprise_id,illegal_type_code"

        val integerType = Array("vehicle_id", /*"type",*/ "state", "create_user", "dept_id", "department_id")
        val timestampType = Array("illegal_date", "create_time")
        val doubleType = Array("enterprise_score", "vehicle_driver_score", "enterprise_final_score", "vehicle_driver_final_score",
            "penalty_amount", "speed")
        val longType = Array("id")

        val fields = schemaString.split(",")
            .map(fieldName => {
                if(integerType.contains(fieldName.trim)) StructField(fieldName, IntegerType, nullable = true)
                else if(timestampType.contains(fieldName.trim)) StructField(fieldName, TimestampType, nullable = true)
                else if(doubleType.contains(fieldName.trim)) StructField(fieldName, DoubleType, nullable = true)
                else if(longType.contains(fieldName.trim)) StructField(fieldName, LongType, nullable = true)
                else StructField(fieldName, StringType, nullable = true)
            })
        val schema = StructType(fields)

        var newDF = spark.createDataFrame(rdd2, schema)
        newDF = newDF.drop("vehicle_id").drop("enterprise_id")
        newDF.createOrReplaceTempView("tmp")
        newDF = spark.sql(
            """
              |select a.*, b.province_id, b.area_id, c.driver_id, d.vehicle_id, e.sn, b.enterprise_id from tmp a
              |     left join dim_cwp_d_enterprise_info b on a.enterprise_name=b.enterprise_name
              |     left join dim_cwp_d_driver_info c on a.driver_name=c.driver_name
              |     left join dim_cwp_d_vehicle_info d on a.car_card_number=d.car_card_number
              |     left join tbl_credit_illegal_details_interface e on a.id=e.id
              |
              |""".stripMargin)
        //where b.enterprise_type_id=3

        var columns = ""
        for(col <- newDF.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(newDF)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_credit_illegal_detail"),
            list, outProPath)

        println("ods_cwp_credit_illegal_detail======================ok")
    }
}
