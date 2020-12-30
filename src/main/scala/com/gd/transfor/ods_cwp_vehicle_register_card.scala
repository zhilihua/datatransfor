package com.gd.transfor

import java.text.SimpleDateFormat

import com.gd.udf.{OdsCwpVehicleRegisterCardUdf, OdsCwpVehicleTransportCardUdf}
import com.gd.util.{DfTransferUtil, MyJDBCUtil, TimeOperate}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DateType, IntegerType, StringType, StructField, StructType}

object ods_cwp_vehicle_register_card {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_twoway_reg_card = MyJDBCUtil.readData(spark, "tbl_twoway_reg_card", inProPath)
        val df_dim_cwp_d_build_site_info = MyJDBCUtil.readData(spark, "dim_cwp_d_build_site_info", outProPath)
        val df_ods_cwp_transportation_contract = MyJDBCUtil.readData(spark, "ods_cwp_transportation_contract", outProPath)    //清运合同
        val df_tbl_vehicleinfo = MyJDBCUtil.readData(spark, "tbl_vehicleinfo", inProPath)     //车辆信息
        //注册视图
        df_tbl_twoway_reg_card.createOrReplaceTempView("tbl_twoway_reg_card")
        df_dim_cwp_d_build_site_info.createOrReplaceTempView("dim_cwp_d_build_site_info")
        df_ods_cwp_transportation_contract.createOrReplaceTempView("ods_cwp_transportation_contract")
        df_tbl_vehicleinfo.createOrReplaceTempView("tbl_vehicleinfo")

        val yesterday = TimeOperate.getYesterday  //'2020-07-24'
        //println(yesterday)
        //注册udf函数
        spark.udf.register("addCoords", OdsCwpVehicleRegisterCardUdf.addCoords _)
        spark.udf.register("changeLines", OdsCwpVehicleRegisterCardUdf.changeLines _)

        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        val sql1 =
            s"""
               |select addtime, a.id, cid as vehicle_id, applyid as apply_id, a.transportunitid as transport_enterprise_id,
               |       a.builderid as build_enterprise_id, a.constructionid as construction_enterprise_id, a.transid as clean_contract_id,
               |       a.buildingsiteid as build_site_id, a.disposaltype as disposal_type, a.disposalsiteid as disposal_site_id,
               |       a.disposalunitid as disposal_enterprise_id, changeLines(a.drivingline) as plan_line_desc,
               |       addCoords(a.lng, a.lat) as coords,
               |       to_date(a.begindate, "yyyy-MM-dd") as start_date, to_date(a.enddate, "yyyy-MM-dd") as end_date,
               |       Issuingunit as issuance_enterprise_id,
               |       to_date(lssuingdate, "yyyy-MM-dd") as issuance_date, licencephoto as certification_picture,
               |       cancel_datetime as invalid_date,
               |       reason as invalid_cause, a.bak as notes, a.disposalconid as disposal_contract_id, a.taskplan as transport_plan,
               |       a.greenpact as environment_agreement, a.recovery_state as recovery_state, number, print_datetime as print_date,
               |       a.garbagetype as garbage_type, a.transportnum as transport_number, start_name, start_lng_lat, start_address,
               |       start_type, end_name, end_lng_lat, end_address, end_type, a.state, number as apply_number, 2 as dept_id, c.department_id
               |from tbl_twoway_reg_card a
               |     left join dim_cwp_d_build_site_info c on a.buildingsiteid=c.build_site_id
               |
               |""".stripMargin  //$yesterday 2020-07-24
//        where date(a.begindate)>'2019-12-01'
        import spark.implicits._
        var df = spark.sql(sql1)
        //df.show()
        df = df.filter($"apply_id" isNotNull)

        //去重
        val rdd = df.rdd

        val rdd1 = rdd.map(
            line => ((line(2), line(3)), line) //将设备id作为键，整体作为值，然后一个个过滤
        ).groupByKey()

        val rdd2 = rdd1.map(data => {
            var res: Row = null
            val mid = data._2.toList

            if (mid.length <= 1) res = mid.head
            else {
                var flag = true
                val lengths = mid.length
                var start = 0
                var maxV: Row = mid.head
                //寻找第一个不为空的开始
                for (i <- 0 until lengths) {
                    if (flag && mid(i)(0) != "" && mid(i)(0) != null) {
                        start = i
                        maxV = mid(i)
                        flag = false
                    }
                }

                //处理null
                for (i <- start until lengths) {
                    if (mid(i)(0) != null && mid(i)(0) != "" && dateFormat.parse(mid(i)(0).toString).getTime -
                        dateFormat.parse(maxV(0).toString).getTime > 0) {
                        maxV = mid(i)
                    }
                }
                res = maxV
            }

            res
        })

        //转换为DF
        val schemaString = "addtime,id,vehicle_id,apply_id,transport_enterprise_id," +
            "build_enterprise_id,construction_enterprise_id,clean_contract_id,build_site_id,disposal_type," +
            "disposal_site_id,disposal_enterprise_id,plan_line_desc,coords,start_date," +
            "end_date,issuance_enterprise_id,issuance_date,certification_picture," +
            "invalid_date,invalid_cause,notes,disposal_contract,transport_plan,environment_agreement," +
            "recovery_state,number,print_date,garbage_type,transport_number," +
            "start_name,start_lng_lat,start_address,start_type,end_name," +
            "end_lng_lat,end_address,end_type,state,apply_number,dept_id,department_id"

        val integerType = Array("vehicle_id", "disposal_type", "transport_number",
            /*"start_type",*/ "recovery_state", /*"end_type",*/ "state", "dept_id", "department_id")
        val dateType = Array("start_date", "end_date", "issuance_date")
        val boolType = Array("start_type", "end_type")

        val fields = schemaString.split(",")
            .map(fieldName => {
                if (integerType.contains(fieldName.trim)) StructField(fieldName, IntegerType, nullable = true)
                else if (dateType.contains(fieldName.trim)) StructField(fieldName, DateType, nullable = true)
                else if (boolType.contains(fieldName.trim)) StructField(fieldName, BooleanType, nullable = true)
                else StructField(fieldName, StringType, nullable = true)
            })
        val schema = StructType(fields)

        var newDF = spark.createDataFrame(rdd2, schema)

        //将false和true替换为0、1
        import org.apache.spark.sql.functions._
        val bool2Int = udf(OdsCwpVehicleRegisterCardUdf.bool2Int _)
        newDF = newDF.withColumn("start_type", bool2Int(newDF("start_type")))
        newDF = newDF.withColumn("end_type", bool2Int(newDF("end_type")))
        newDF = newDF.drop("addtime")

        newDF.createOrReplaceTempView("tmp")
        spark.udf.register("changeInvol", (x: Int) => {
            x match {
                case 3 => 1
                case _ => 0
            }
        })
        spark.udf.register("changeVTI", (x: Int) => {
            x match {
                case 16 => 0
                case 17 => 1
                case _ => 3
            }
        })
        newDF = spark.sql(
            """
              |select a.*, changeInvol(b.disposal_type) as is_invol, changeVTI(c.VType) as vehicle_type_id from tmp a
              |     left join ods_cwp_transportation_contract b on a.clean_contract_id=b.id
              |     left join tbl_vehicleinfo c on a.vehicle_id=c.CID
              |""".stripMargin)

        var columns = ""
        for(col <- newDF.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(newDF)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_vehicle_register_card"),
            list, outProPath)

        println("ods_cwp_vehicle_register_card======================ok")
    }
}
