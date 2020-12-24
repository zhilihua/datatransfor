package com.gd.util

import java.sql.DriverManager
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object MyJDBCUtil extends Serializable {
    /**
     * 用于读取mysql数据库中的数据
     * @param sparkSession spark驱动
     * @param tableName 读取表的名字
     * @param proPath 配置文件路径
     */
    def readData(sparkSession: SparkSession, tableName: String, proPath: String): DataFrame ={
        val prop = getProperties(proPath)
        sparkSession
            .read
            .format("jdbc")
            .option("url", prop.getProperty("mysql.url"))
            .option("driver", prop.getProperty("mysql.driver"))
            .option("user", prop.getProperty("mysql.username"))
            .option("password", prop.getProperty("mysql.password"))
            .option("dbtable", tableName)
            .load()
    }

    /**
     * 保存数据到数据库
     * @param sql： 要执行的sql语句
     */
    def updateData(sql: String, map: Map[String, Any], proPath: String): Unit ={
        //进行sql转换
        var sql2: String = sql
        for(key <- map.keys) {
            if(map(key) == null) {
                sql2 = sql2.replaceFirst(":"+key+", |" +",[\\s]*:"+key+"", "")
                sql2 = sql2.replaceFirst(""+key+", |" +",[\\s]*"+key+"", "")
            }else{
                sql2 = sql2.replaceFirst(":"+key, "'"+map(key).toString+"'")
            }
        }

        val prop = getProperties(proPath)
        val url = prop.getProperty("mysql.url")
        val username = prop.getProperty("mysql.username")
        val password = prop.getProperty("mysql.password")

        Class.forName("com.mysql.jdbc.Driver");
        val con = DriverManager.getConnection(url, username, password)
        val st = con.createStatement()
        st.executeUpdate(sql2)

        st.close()
        con.close()
        println(sql2)
    }

    /**
     * 批量写入数据库
     * @param sql： 要执行的sql语句
     * @param list： 要插入的数据
     * @param proPath： 配置文件路径
     */
    def updateDataList(sql: String, list: List[Map[String, Any]], proPath: String): Unit ={
        //获取与mysql连接
        val prop = getProperties(proPath)
        val url = prop.getProperty("mysql.url")
        val username = prop.getProperty("mysql.username")
        val password = prop.getProperty("mysql.password")

        Class.forName("com.mysql.jdbc.Driver");
        val con = DriverManager.getConnection(url, username, password)
        val st = con.createStatement()

        var i = 0
        for(lst <- list) {
            i += 1
            //进行sql转换
            var sql2: String = sql
            for(key <- lst.keys) {
                if(lst(key) == null) {
                    sql2 = sql2.replaceFirst(":"+key+", |" +",[\\s]*:"+key+"", "")
                    sql2 = sql2.replaceFirst(""+key+", |" +",[\\s]*"+key+"", "")
                    sql2 = sql2.replace("`" + key + "`,", "")
                }else{
                    sql2 = sql2.replaceFirst(":"+key, "'"+lst(key).toString+"'")
                }
            }
            st.addBatch(sql2)
            if(i % 200 == 0){
                st.executeBatch()
                st.clearBatch()
            }
//            st.executeUpdate(sql2)
        }
        st.executeBatch()

        st.close()
        con.close()
    }

    def delDataList(table: String, proPath: String, field: String): Unit = {
        val prop = getProperties(proPath)
        val url = prop.getProperty("mysql.url")
        val username = prop.getProperty("mysql.username")
        val password = prop.getProperty("mysql.password")

        Class.forName("com.mysql.jdbc.Driver")
        val con = DriverManager.getConnection(url, username, password)

        val sql =
            s"""
              |delete from ${table} where ${field}=2
              |""".stripMargin

        val ps = con.prepareStatement(sql)
        ps.executeUpdate()

        ps.close()
        con.close()

        println(s"${table}=====================clear")
    }

    def updateSingerData(sql: String, proPath: String): Unit = {
        val prop = getProperties(proPath)
        val url = prop.getProperty("mysql.url")
        val username = prop.getProperty("mysql.username")
        val password = prop.getProperty("mysql.password")

        Class.forName("com.mysql.jdbc.Driver")
        val con = DriverManager.getConnection(url, username, password)

        val ps = con.prepareStatement(sql)
        ps.executeUpdate()

        ps.close()
        con.close()

        println(s"========================update=======================")
    }

    /**
     * @param proPath 配置文件路径
     * @return
     */
    def getProperties(proPath: String):Properties = {
        val path = MyJDBCUtil.getClass.getClassLoader.getResourceAsStream(proPath)
        val properties: Properties = new Properties()
        properties.load(path)
        properties
    }



    def main(args: Array[String]): Unit = {
        val map = Map("name" -> "xm", "age" -> 15, "class" -> null, "date" -> "2020-11-12", "timestamp" -> "2020-12-12 12:34:50", "double" -> "2.41")

        val map1 = Map("name" -> "xq", "age" -> 15, "class" -> "一年级", "date" -> "2020-11-12", "timestamp" -> "2020-12-12 12:34:50", "double" -> null)

        val lsts = List(map, map1)

        val sql1 =
            """
              |replace into aaa (name, age, class, date, timestamp, double)values (:name, :age, :class, :date, :timestamp, :double)
              |""".stripMargin

//        updateData(sql1, map, "input.properties")

        updateDataList(sql1, lsts, "input.properties")
    }
}
