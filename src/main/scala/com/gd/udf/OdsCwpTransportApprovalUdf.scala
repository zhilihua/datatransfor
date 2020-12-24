package com.gd.udf

object OdsCwpTransportApprovalUdf {
    def changeState(x: Int): Int = {
        var res = 0
        if (x == 0) res = 1
        else if (x == 1) res = 5
        res
    }
}
