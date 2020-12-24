package com.gd.udf

object OdsCwpInformationManageUdf {
    def transferType(x: Int): Int = {
        var res = -1
        if (x == 83) res = 1
        else if (x == 84) res = 2
        else if (x == 85) res = 3
        else if (x == 86) res = 4

        res
    }
}
