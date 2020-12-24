package com.gd.udf

object DimCwpDDisposalSiteInfoUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0

    def addProvince(x: Int): String = {
        x.toString.substring(0, 2) + "0000"
    }

    def addCity(x: Int): String = {
        x.toString.substring(0, 4) + "00"
    }

    def changeLng(lng: Double, lat: Double) = {
        val x = lng
        val y = lat
        val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
        val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
        val bdLng = z * Math.cos(theta) + 0.0065
        bdLng
    }

    def changeLat(lng: Double, lat: Double) = {
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
            case 1 => 2
            case _ => 2
        }
    }
}
