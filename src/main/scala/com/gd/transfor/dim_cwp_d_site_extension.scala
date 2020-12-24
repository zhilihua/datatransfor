package com.gd.transfor

import java.text.SimpleDateFormat

import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object dim_cwp_d_site_extension {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_dregs_source_location = MyJDBCUtil.readData(spark, "tbl_dregs_source_location", inProPath)
        val df_tbl_dregs_source_location_zhujian_kcb = MyJDBCUtil.readData(spark, "tbl_dregs_source_location_zhujian_kcb", inProPath)
        val df_sys_department = MyJDBCUtil.readData(spark, "sys_department", outProPath)
        //注册视图
        df_tbl_dregs_source_location.createOrReplaceTempView("tbl_dregs_source_location")
        df_tbl_dregs_source_location_zhujian_kcb.createOrReplaceTempView("tbl_dregs_source_location_zhujian_kcb")
        df_sys_department.createOrReplaceTempView("sys_department")
        //定义sql
        val sql1 =
            """
              |select a.id as site_id, b.bsiteNo as external_site_id, 2 as dept_id, create_time, d.id as department_id
              |from tbl_dregs_source_location a
              |     left join tbl_dregs_source_location_zhujian_kcb b on a.prj_id=b.prj_id
              |     left join (select e.id, e.area_id from sys_department e where e.id < 410002 or e.id > 411001) d on a.departmentid=d.area_id
              |""".stripMargin

        val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val df = spark.sql(sql1)
        val rdd = df.rdd

        val rdd1 = rdd.map(line => (line(0), line)).groupByKey()

        val rdd2 = rdd1.map(data => {
            var res: Row = null
            val mid = data._2.toList
            if(mid.length <= 1) res = mid.head
            else {
                var flag = true
                val lengths = mid.length
                var start = 0
                var maxV = mid.head
                //寻找第一个不为空的开始
                for(i <- 0 until lengths){
                    if(flag && mid(i)(3) != "" && mid(i)(3) != null) {
                        start = i
                        maxV = mid(i)
                        flag=false
                    }
                }

                //处理null
                for(i <- start until lengths){
                    if(mid(i)(3) != null && mid(i)(3) != "" && dateFormat.parse(mid(i)(3).toString).getTime -
                        dateFormat.parse(maxV(3).toString).getTime > 0){
                        maxV = mid(i)
                    }
                }
                res = maxV
            }
            res
        })

        val schema = StructType(Array(StructField("site_id", StringType, nullable = true),
            StructField("external_site_id", StringType, nullable = true),
            StructField("dept_id", IntegerType, nullable = true),
            StructField("create_time", TimestampType, nullable = true),
            StructField("department_id", IntegerType, nullable = true)
        ))

        var dfF = spark.createDataFrame(rdd2, schema)
        dfF = dfF.drop("create_time")
        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(getSql, list, outProPath)

        println("dim_cwp_d_site_extension==============ok")
    }

    def getSql = {
        val fields =
            """
              |site_id,external_site_id,dept_id,department_id
              |""".stripMargin
        val sql =
            s"""
              |replace into dim_cwp_d_site_extension (${fields})
              |values(${DfTransferUtil.changeFields(fields)})
              |""".stripMargin
        sql
    }
}
