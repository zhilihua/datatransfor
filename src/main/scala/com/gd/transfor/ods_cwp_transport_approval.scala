package com.gd.transfor

import com.gd.udf.OdsCwpTransportApprovalUdf
import com.gd.util.{DfTransferUtil, MyJDBCUtil}
import org.apache.spark.sql.SparkSession

object ods_cwp_transport_approval {
    private val inProPath = "input.properties"
    private val outProPath = "output.properties"

    def run(spark: SparkSession) = {
        //获取数据
        val df_tbl_comtransport_approval = MyJDBCUtil.readData(spark, "tbl_comtransport_approval", inProPath)
        val df_tbl_company_transpermit = MyJDBCUtil.readData(spark, "tbl_company_transpermit", inProPath)
        val df_sys_user = MyJDBCUtil.readData(spark, "sys_user", outProPath)
        //注册视图
        df_tbl_comtransport_approval.createOrReplaceTempView("tbl_comtransport_approval")
        df_tbl_company_transpermit.createOrReplaceTempView("tbl_company_transpermit")
        df_sys_user.createOrReplaceTempView("sys_user")
        //注册udf
        spark.udf.register("changeState", OdsCwpTransportApprovalUdf.changeState _)
        //定义sql
        val sql1 =
            """
              |select a.id, a.jgunitid as area_id, a.name as apply_enterprise_id, a.legalperson as legal_person,
              |     a.phone as legal_person_phone,
              |     a.addr as address, a.applyitems as apply_item, a.seviceitems as sevice_item, a.engagednum as engaged_number,
              |     a.majornum as major_number, a.equipment as equipment, a.applynum as apply_number, c.id as apply_user,
              |     to_timestamp(a.applydate, "yyyy-MM-dd") as apply_time, a.applydesc as apply_desc,
              |     a.applyphoto as apply_picture, a.IDphoto as id_picture,
              |     a.lepersonphoto as legal_picture, a.proxyphoto as entrustment_picture, b.id as create_user,
              |     to_timestamp(b.addtime, "yyyy-MM-dd HH:mm:ss") as create_time, a.approvalresult as approval_opinion,
              |     f.id as approval_user,
              |     to_timestamp(a.approvaltime, "yyyy-MM-dd HH:mm:ss") as approval_time, b.licencekey as approval_card_number,
              |     to_date(b.startdate, "yyyy-MM-dd") as start_date, to_date(b.enddate, "yyyy-MM-dd") as end_date,
              |     b.licencephoto as approval_card_picture, to_timestamp(b.addtime, "yyyy-MM-dd HH:mm:ss") as issue_time,
              |     b.reason as invalid_reason, to_timestamp(b.cancel_datetime, "yyyy-MM-dd HH:mm:ss") as invalid_time,
              |     changeState(b.state) as state, b.Issuingunit as department_id, 2 as dept_id
              |from tbl_comtransport_approval a
              |     left join (select d.applyid, e.id, d.addtime, d.startdate, d.licencekey, d.enddate, d.licencephoto,
              |                     d.addtime, d.reason, d.cancel_datetime, d.state, d.Issuingunit
              |                from tbl_company_transpermit d
              |                     left join sys_user e on d.djuserid=e.username) b on a.id=b.applyid
              |     left join sys_user c on a.applyuserid=c.username
              |     left join sys_user f on a.approvalaccount=f.username
              |     where c.dept_id=2
              |""".stripMargin
        val df = spark.sql(sql1)
        var columns = ""
        for(col <- df.columns){
            columns += col+","
        }
        columns = columns.substring(0, columns.length-1)

        val list = DfTransferUtil.df2Map(df)
        MyJDBCUtil.updateDataList(DfTransferUtil.getSql(columns, "ods_cwp_transport_approval"),
            list, outProPath)

        println("ods_cwp_transport_approval================ok")
    }
}
