package com.gd.udf

import scala.collection.mutable.ArrayBuffer

object DimCwpBoundaryConditionUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
    def addCoords(x: String, y: String): String = {
        val lst1 = x.split(",")
        val lst2 = y.split(",")

        var res = "["
        for (i <- 0 until lst1.length) {
            res = res + "[" + lst1(i) + "," + lst2(i) + "],"
        }
        res.substring(0, res.length-1) + "]"
    }

    def changeLng(lng: String, lat: String): String = {
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

    def changeLat(lng: String, lat: String): String = {
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
}
