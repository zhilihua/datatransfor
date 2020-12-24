package com.gd.udf

import java.util.UUID

object OdsCwpDisposalApprovalUdf {
    def getUUID(x: String): String = {
        var res: String = null
        if (x == null) res = UUID.randomUUID().toString.replace("-", "")
        else res = x
        res
    }

    def getState(x: Int, y: Int): Int = {
        var res = -1
        y match {
            case 1 =>
                res = 5
            case _ =>
                x match {
                    case  0|1 => res = 0
                    case 2 => res = 10
                    case 3 => res = 11
                    case 4 => res = 7
                    case 5 => res = 6
                    case 6 => res = 8
                    case 7 => res = 9
                    case 8 => res = 4
                    case 9 => res = 3
                }
        }

        res
    }
}
