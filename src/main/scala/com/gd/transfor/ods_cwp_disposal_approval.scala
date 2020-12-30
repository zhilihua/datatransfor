package com.gd.transfor

import com.gd.udf.OdsCwpDisposalApprovalUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_disposal_approval {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_companytran_approval = MyJDBCUtil.readData(spark, "tbl_companytran_approval", inProPath)
        val df_tbl_consumptivefield_permit = MyJDBCUtil.readData(spark, "tbl_consumptivefield_permit", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_companytran_approval.createOrReplaceTempView("tbl_companytran_approval")
        df_tbl_consumptivefield_permit.createOrReplaceTempView("tbl_consumptivefield_permit")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("getUUID", OdsCwpDisposalApprovalUdf.getUUID _)
        spark.udf.register("getState", OdsCwpDisposalApprovalUdf.getState _)
        //定义sql
        val sql1 =
            """
              |select getUUID(b.id) as id, a.jgunitid as area_id, a.name as apply_enterprise_id, a.legalperson as legal_person,
              |     a.phone as legal_person_phone,
              |     a.addr as address, b.disposalsiteid as disposal_site_id, a.applyitems as apply_item,
              |     a.seviceitems as sevice_item,
              |     a.applynum as apply_number, a.equipment, a.equipmentnum as equipment_number,
              |     a.applyphoto as apply_picture, a.IDphoto as id_picture, a.lepersonphoto as legal_picture,
              |     a.proxyphoto as entrustment_picture, a.applydesc as apply_desc, c.id as apply_user,
              |     to_date(a.applydate, "yyyy-MM-dd") as apply_time, to_date(b.lssuingdate, "yyyy-MM-dd") as issue_time,
              |     to_timestamp(a.addtime, "yyyy-MM-dd HH:mm:ss") as create_time,
              |     a.approvalresult as approval_opinion, d.id as approval_user,
              |     a.approvaltime as approval_time, b.licencekey as approval_card_number,
              |     to_date(b.startdate, "yyyy-MM-dd") as start_date, to_date(b.enddate, "yyyy-MM-dd") as end_date,
              |     b.licencephoto as approval_card_picture, e.id as public_user,
              |     to_timestamp(a.publictime, "yyyy-MM-dd HH:mm:ss") as public_time,
              |     b.reason as invalid_reason, b.invalid_user,
              |     to_timestamp(b.cancel_datetime, "yyyy-MM-dd HH:mm:ss") as invalid_time,
              |     getState(a.state, b.state) as state, b.Issuingunit as department_id, 2 as dept_id
              |from tbl_companytran_approval a
              |     right join (select f.*, g.id as invalid_user
              |                 from tbl_consumptivefield_permit f
              |                     left join sys_user g on f.djuserid=g.username) b on a.id=b.applyid
              |     left join sys_user c on a.applyuserid=c.username
              |     left join sys_user d on a.approvalaccount=d.username
              |     left join sys_user e on a.publicaccount=e.username
              |""".stripMargin
        var df = spark.sql(sql1)
        df = df.na.fill(value = "410100", cols = Array("area_id"))
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_disposal_approval"),
            list, outProPath)

        println("ods_cwp_disposal_approval==================ok")
    }
}
