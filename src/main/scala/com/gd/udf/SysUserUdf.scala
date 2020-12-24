package com.gd.udf

object SysUserUdf {
    def getEnterpriseId(x: Int, y: String): String = {
        var res: String = null
        if (x != 9) res = y
        res
    }

    def getDepartmentId(x: Int, y: String): String = {
        var res: String = "410001"
        if (x == 9) res = y
        res
    }

    def gdMD5(resultString: String): String ={
        val sb = new StringBuffer()
        sb.append(resultString.substring(16, 24));
        sb.append(resultString.substring(0, 8));
        sb.append(resultString.substring(8, 16));
        sb.append(resultString.substring(24));

        sb.toString
    }

    def changeStatus(x: Any): Int = {
        x match {
            case 1 => 0
            case _ => 1
        }
    }

    def addDepartment(x: Int): Int = {
        x match {
            case a if a >= 1 && a <= 4 => 1
            case b if b == 9 => 0
            case c: Int => c
        }
    }

    def addEnterpriseName(x: String, y: String): String = {
        var res: String = null
        if (x != null) res = x
        else if (y != null) res = y
        res
    }
}
