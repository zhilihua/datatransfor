package com.gd.transfor

import java.text.SimpleDateFormat

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object dim_cwp_d_terminal_info {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_devinfo = MyJDBCUtil.readData(spark, "tbl_devinfo", inProPath)
        val df_tbl_uservehicle = MyJDBCUtil.readData(spark, "tbl_uservehicle", inProPath)
        val df_dim_cwp_d_enterprise_info = MyJDBCUtil.readData(spark, "dim_cwp_d_enterprise_info", outProPath)
        //注册视图
        df_tbl_devinfo.createOrReplaceTempView("tbl_devinfo")
        df_tbl_uservehicle.createOrReplaceTempView("tbl_uservehicle")
        df_dim_cwp_d_enterprise_info.createOrReplaceTempView("dim_cwp_d_enterprise_info")
        //定义sql
        val sql1 =
            """
              |select SN as terminal_sn, a.DevID as terminal_id, Type as terminal_type, Status as terminal_state,
              |     TFlag as t_flag, DFLAG as d_flag, Brand as terminal_brand, Model as terminal_model, mobile as phone,
              |     ExtInfo as extend_info, Remark as notes, platformtype as enterprise_name, addtime as add_date,
              |     hkid as hk_id, CfgID as cfg_id, b.GrpID as enterprise_id, b.department_id, 2 as dept_id
              |from tbl_devinfo a
              |     left join (select c.devid, c.GrpID, d.department_id from tbl_uservehicle c
              |                     left join dim_cwp_d_enterprise_info d on c.GrpID=d.enterprise_id) b on a.DevId=b.DevId
              |""".stripMargin
        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        var df = spark.sql(sql1)
        //逻辑操作
        val rdd = df.rdd

        val rdd1 = rdd.map(line => (line(1), line)).groupByKey()

        val rdd2 = rdd1.map(data => {
            var res: Row = null
            val mid = data._2.toList
            if (mid.length <= 1) res = mid.head
            else {
                var flag = true
                val lengths = mid.length
                var start = 0
                var maxV = mid.head
                //寻找第一个不为空的开始
                for (i <- 0 until lengths) {
                    if (flag && mid(i)(12) != "" && mid(i)(12) != null) {
                        start = i
                        maxV = mid(i)
                        flag = false
                    }
                }

                //处理null
                for (i <- start until lengths) {
                    if (mid(i)(12) != null && mid(i)(12) != "" && dateFormat.parse(mid(i)(12).toString).getTime -
                        dateFormat.parse(maxV(12).toString).getTime > 0) {
                        maxV = mid(i)
                    }
                }
                res = maxV
            }
            res
        })
        //rdd转df
        val columnNames = "terminal_sn,terminal_id,terminal_type,terminal_state," +
            "t_flag,d_flag,terminal_brand,terminal_model," +
            "phone,extend_info,notes,enterprise_name," +
            "add_date,hk_id,cfg_id,enterprise_id," +
            "department_id,dept_id"
        val integerType = Array("dept_id", "department_id")
        val longType = Array("terminal_type", "terminal_state", "t_flag", "cfg_id", "d_flag")
        val fields = columnNames.split(",").map(fieldName => {
            if (integerType.contains(fieldName.trim)) StructField(fieldName, IntegerType, nullable = true)
            else if (longType.contains(fieldName.trim)) StructField(fieldName, LongType, nullable = true)
            else StructField(fieldName, StringType, nullable = true)
        })
        val schema = StructType(fields)

        val dfF = spark.createDataFrame(rdd2, schema)
        val list = DfTransferUtil.df2Map(dfF)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_terminal_info==============ok")
    }

    def getSql = {
        val fields =
            """
              |terminal_sn,terminal_id,terminal_type,terminal_state,t_flag,d_flag,
              |terminal_brand,terminal_model,phone,extend_info,notes,enterprise_name,
              |add_date,hk_id,cfg_id,enterprise_id,department_id,dept_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_terminal_info (${fields})
              |values (${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
