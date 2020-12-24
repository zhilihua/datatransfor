package com.gd.udf

object OdsCwpCreditIllegalRegistrationUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0

    def getUrl(x: String, y: Int, flag: String): String = {
        var res: String = null
        if (flag == "vehicle") {
            if (y == 2) res = x
        }
        else if (flag == "driving") {
            if (y == 1) res = x
        }
        else if (flag == "scene") {
            if (y == 3) res = x
        }
        res
    }

    def changeApp(x: Any): String = {
        var res = "-"
        if (x == 0 || x == 3) res = 0.toString
        else if (x == 2) res = 1.toString
        res
    }

    def getUuid(x: Int, y: Int): String = {
        var res: String = null
        if (y == 2) res = x.toString
        res
    }

    def getState(x: Int): Int = {
        var res = x
        x match {
            case 4 => res = 3
            case 7 => res = 5
            case _ =>
        }
        res
    }

    def getInt(x: Int): Int = {
        x
    }

    def SingerNotNullStringChangeLng(lng: String, lat: String): String = {
        if (lng == null || lng.isEmpty || lng == "null" || lat.isEmpty || lat == "null" || lat == null) {
            null
        } else {
            val x = lng.toDouble
            val y = lat.toDouble
            val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
            val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
            val bdLng = z * Math.cos(theta) + 0.0065
            bdLng.toString
        }

    }

    def SingerNotNullStringChangeLat(lng: String, lat: String): String = {
        if (lng == null || lng.isEmpty || lng == "null" || lat.isEmpty || lat == "null" || lat == null) {
            null
        } else {
            val x = lng.toDouble
            val y = lat.toDouble
            val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
            val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
            val bdLat = z * Math.sin(theta) + 0.006
            bdLat.toString
        }
    }
}
