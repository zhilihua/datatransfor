package com.gd.udf

object SysDepartmentUdf {
    def defaultInt(x: Any): Int = {
        var res = 0
        if (x != "" && x != null) res = x.toString.toInt
        res
    }

    def addPath(x: String, y: String): String = {
        var res: String = null
        if (y != null && y != "") res = "/" + y + "/" + x + "/"
        else res = "/"+x+"/"
        res
    }
}
