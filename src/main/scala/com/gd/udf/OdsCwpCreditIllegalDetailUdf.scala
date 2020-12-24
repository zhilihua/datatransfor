package com.gd.udf

object OdsCwpCreditIllegalDetailUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
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
