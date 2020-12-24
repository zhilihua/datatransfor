package com.gd.udf

object OdsCwpVehicleTransportCardUdf {
    def changeState(x: Any): Int = {
        x match {
            case 0 => 1
            case 1 => 3
            case _ => 0
        }
    }
}
