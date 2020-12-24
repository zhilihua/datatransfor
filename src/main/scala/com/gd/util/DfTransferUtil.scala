package com.gd.util

import org.apache.spark.sql.DataFrame

object DfTransferUtil {
    def df2Map(df: DataFrame) = {
        val columns = df.columns
        val lst = df.collect().map(row => {
            val seq = row.toSeq
            columns.zip(seq)
        }).map(arr => {
            arr.map(
                info => {
                    info._2 match {
                        case null =>
                            (info._1, null)
                        case _ =>
                            (info._1, info._2.toString)
                    }
                }
            ).toMap
        }).toList

        lst
    }

    def changeFields(fields: String) = {
        var reStr = ""

        val strings = fields.split(",")
        for (str <- strings) {
            if(str.contains("`")){
                val str1 = str.replace("`", "")
                reStr += ":"+str1.trim+","
            }else{
                reStr += ":"+str.trim+","
            }
        }
        reStr = reStr.substring(0, reStr.length-1)
        reStr
    }

    def getSql(fields: String, table: String) = {
        val sql =
            s"""
               |replace into ${table} (${fields})
               |values(${DfTransferUtil.changeFields(fields)})
               |""".stripMargin
        sql
    }

    def main(args: Array[String]): Unit = {
        println(changeFields("id,name,`remark`,create_time,update_time,auth_type,sys_dept_id"))
    }
}
