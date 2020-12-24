package com.gd.udf

object OdsCwpSendCarUdf {
    def mergeState(x: Int, y: Any): Int = {
        var res: Int = 0
        if (y == 1) res = 4
        else if (y == 2) res = 5
        else if (x == 2) res = 3
        res
    }
}
