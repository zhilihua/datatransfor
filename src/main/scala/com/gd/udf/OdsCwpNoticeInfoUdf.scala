package com.gd.udf

object OdsCwpNoticeInfoUdf {
    def transferType(x: Int): Int = {
        var res = -1
        if (x == 81) res = 1
        else if (x == 82) res = 2

        res
    }
}
