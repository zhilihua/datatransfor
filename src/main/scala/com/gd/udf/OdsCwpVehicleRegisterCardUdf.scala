package com.gd.udf

import scala.collection.mutable.ArrayBuffer

object OdsCwpVehicleRegisterCardUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0

    //公共的经纬度转换===============================
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




    def addCoords(x: String, y: String): String = {
        val res = new ArrayBuffer[String]
        val lngs = x.split(";")
        val lats = y.split(";")
        for (i <- 0 until lngs.length if lngs(i) != "") {
            val lngSub = lngs(i)
            val latSub = lats(i)
            val lngf = StringChangeLng(lngSub, latSub)
            val latf = StringChangeLat(lngSub, latSub)

            val lng = lngf.split(",")
            val lat = latf.split(",")
            val mid = lng.zip(lat).mkString("[", ",", "]").
                replace("(", "[").replace(")", "]")
            res.append(mid)
        }
        res match {
            case a if a.length == 1 => "["+a(0)+"]"
            case b if b.length > 1 => b.mkString("[", ",", "]")
            case _ => null
        }
    }


    def bool2Int(x: Boolean): Int = {
        var res = 0
        if (x) res = 1
        res
    }

    def changeLines(x: String): String ={
        val res = new ArrayBuffer[String]
        val lines = x.split("</br>")
        for(line <- lines){
            res.append('"'+line+'"')
        }
        res match {
            case a if a.length == 1 => '[' + a(0) + ']'
            case b if b.length > 1 => b.mkString("[", ",", "]")
            case _ => null
        }

    }
}
