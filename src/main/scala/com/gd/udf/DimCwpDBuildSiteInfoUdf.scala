package com.gd.udf

import scala.collection.mutable.ArrayBuffer

object DimCwpDBuildSiteInfoUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
    def StringChangeLng(lng: String, lat: String): String = {
        val res = new ArrayBuffer[String]
        val lngs = lng.split(",")
        val lats = lat.split(",")

        for (i <- 0 until lngs.length) {
            val x = lngs(i).toDouble
            val y = lats(i).toDouble
            val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
            val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
            val bdLng = z * Math.cos(theta) + 0.0065

            res.append(bdLng.toString)
        }
        res.mkString(",")
    }
    def StringChangeLat(lng: String, lat: String): String = {
        val res = new ArrayBuffer[String]
        val lngs = lng.split(",")
        val lats = lat.split(",")

        for (i <- 0 until lngs.length) {
            val x = lngs(i).toDouble
            val y = lats(i).toDouble
            val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
            val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
            val bdLat = z * Math.sin(theta) + 0.006

            res.append(bdLat.toString)
        }
        res.mkString(",")
    }
    """======================================================================================="""

    def addProvince(x: Int): String = {
        x.toString.substring(0, 2) + "0000"
    }

    def addCity(x: Int): String = {
        x.toString.substring(0, 4) + "00"
    }

    def changeLng(lng: Double, lat: Double): Double = {
        val x = lng
        val y = lat
        val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
        val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
        val bdLng = z * Math.cos(theta) + 0.0065
        bdLng
    }

    def changeLat(lng: Double, lat: Double): Double = {
        val x = lng
        val y = lat
        val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
        val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
        val bdLat = z * Math.sin(theta) + 0.006
        bdLat
    }

    def changeState(x: Int): Int = {
        x match {
            case 0 => 1
            case 1 => 0
        }
    }

    def changeLngLat(LngLats: String): String = {
        if (LngLats == null || LngLats.isEmpty || LngLats == "null") {
            null
        } else {
            var res = "["
            val lnglats = LngLats.split(";")
            for (lnglat <- lnglats) {
                val lng = lnglat.split(",")(0)
                val lat = lnglat.split(",")(1)

                val lng1 = StringChangeLng(lng, lat)
                val lat1 = StringChangeLat(lng, lat)
                val mid = "[" + lng1 + "," + lat1 + "],"
                res += mid
            }
            res = res.substring(0, res.length -1)
            res += "]"
            res
        }
    }

    def changeType(x: Int): Int ={
        x match {
            case 1 => 4
            case 2 => 2
            case _ => 0
        }
    }
}
