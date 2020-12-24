package com.gd.udf

object DimCwpDEnterpriseInfoUdf {
    val x_pi: Double = 3.14159265358979324 * 3000.0 / 180.0
    def changeLng(lng: String, lat: String) = {
        lng match {
            case null =>
                null
            case _ =>
                val x = lng.toDouble
                val y = lat.toDouble
                val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
                val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
                val bdLng = z * Math.cos(theta) + 0.0065
                bdLng.toString
        }
    }

    def changeLat(lng: String, lat: String) = {
        lng match {
            case null =>
                null
            case _ => {
                val x = lng.toDouble
                val y = lat.toDouble
                val z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * this.x_pi)
                val theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * this.x_pi)
                val bdLat = z * Math.sin(theta) + 0.006
                bdLat.toString
            }
        }
    }
}
