package com.gd.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/*
    经常会用到日期之间的转换与格式化，这里写一个类完成转换
 */
object TimeOperate {
    //获取今天日期
    def getNowDate: String ={
        var now:Date = new Date()
        var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var today = dateFormat.format( now )
        today
    }

    //获取昨天日期
    def getYesterday:String= {
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        var yesterday = dateFormat.format(cal.getTime)
        yesterday
    }

    //获取任何距离今天任何一天的日期
    def getAnyday(num: Int): String ={
        var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        var cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, num)
        var yesterday = dateFormat.format(cal.getTime)
        yesterday
    }

    //获取年
    def getYear(time: String): String = {
        time.substring(0, 4)
    }
    //获取月份
    def getMonth(time: String): String = {
        time.substring(5, 7)
    }
    //获取日
    def getDay(time: String): String = {
        time.substring(8, 10)
    }

    def main(args: Array[String]): Unit = {
        println(TimeOperate.getNowDate)
        println(TimeOperate.getYesterday)
        println(TimeOperate.getAnyday(1))
        println(TimeOperate.getNowDate.getClass.getSimpleName)
        println(TimeOperate.getYear(TimeOperate.getNowDate))
        println(TimeOperate.getMonth(TimeOperate.getNowDate))
        println(TimeOperate.getDay(TimeOperate.getNowDate))

        var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val t1 = "2010-10-10 10:10:10"
        val t2 = "2010-10-10 10:10:10"
        println(dateFormat.parse(t2).getTime)
        println(dateFormat.parse("2017-08-05 09:39:25").getTime)
    }
}
