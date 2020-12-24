package com.gd.udf

object DimCwpDDriverInfoUdf {
    def getEndTime(x: String, y: Int): String = {
        val arr = x.split("-|[.]") //利用-和.进行分割
        val year = arr(0).toInt + y

        val time = year.toString + "-" + arr(1) + "-" + arr(2)
        time
    }

    def defaultInt(x: Any): Int = {
        var res = 0
        if (x != "" && x != null) {
            var mid = x.toString.toInt
            if (mid < 410001001 || mid >= 410001016) mid = -1
            res = mid
        }

        res
    }
}
