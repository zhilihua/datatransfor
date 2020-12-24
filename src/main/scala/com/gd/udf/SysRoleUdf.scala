package com.gd.udf

object SysRoleUdf {
    def addAuthType(x: String): Int = {
        """0自定义角色、1账套管理员、3. 执法人员 4 处置单位组 5 运输单位组 6 建设单位组 7 施工单位组"""
        x match {
            case zT if zT.contains("账套管理员") => 1
            case zF if zF.contains("执法人员") => 3
            case cZ if cZ.contains("处置单位组") => 4
            case yS if yS.contains("运输单位组") => 5
            case jS if jS.contains("建设单位组") => 6
            case sG if sG.contains("施工单位组") => 7
            case sJ if sJ.contains("司机") => 8
            case cDZ if cDZ.contains("车队长") => 9
            case _ => 0
        }
    }
}
