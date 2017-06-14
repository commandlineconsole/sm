package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryInvolvementInfo
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.HashHelper


object SInvolvementInfoTransformer {
  def involvementInfo(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) : List[SServiceDeliveryInvolvementInfo] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val directInvolvementInfo: List[SServiceDeliveryInvolvementInfo] = serviceDelivery.involvements.sorted.map(involvement => {
      val invlmnt_method_cd = "DIRECT"
      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))
      val chlg_rec_hash_value = HashHelper.sha1(Seq(
        ("messageId",messageId),
        ("src_created_by", involvement.createdUserId.getOrElse("")),
        ("last_updated_by", involvement.lastUpdatedUserId.getOrElse(""))
      ))
      val vis_rec_hash_value = HashHelper.sha1(involvement.internalHandle.visibility_marker.getOrElse(""))
      SServiceDeliveryInvolvementInfo(messageId, involvement.created, fmt.print(time),
        source, serviceDelivery.internalHandle.interface_identifier, involvement.internalHandle.interface_identifier,
        invlmnt_method_cd, chlg_rec_hash_value, involvement.createdUserId,
        involvement.created, involvement.lastUpdatedUserId, involvement.lastUpdated,
        involvement.createdDate, vis_rec_hash_value, involvement.internalHandle.visibility_marker,
        invlmnt_hk)
    })



    val inDirectInvolvementInfo: List[SServiceDeliveryInvolvementInfo] = serviceDelivery.indirectInvolvements.sorted.map(indirectInvolvement => {
      val invlmnt_method_cd = "INDIRECT"
      val chlg_rec_hash_value = HashHelper.sha1(Seq(
        ("messageId",messageId),
        ("src_created_by", indirectInvolvement.createdBy.getOrElse("")),
        ("last_updated_by", indirectInvolvement.lastUpdatedUserId.getOrElse(""))
      ))
      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", indirectInvolvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))
      val vis_rec_hash_value = HashHelper.sha1(indirectInvolvement.internalHandle.visibility_marker.getOrElse(""))
      SServiceDeliveryInvolvementInfo(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        source, serviceDelivery.internalHandle.interface_identifier, indirectInvolvement.internalHandle.interface_identifier,
        invlmnt_method_cd, chlg_rec_hash_value, indirectInvolvement.createdBy,
        indirectInvolvement.created, indirectInvolvement.lastUpdatedUserId, indirectInvolvement.lastUpdated,
        None, vis_rec_hash_value, indirectInvolvement.internalHandle.visibility_marker,
        invlmnt_hk
      )
    })

    directInvolvementInfo ++ inDirectInvolvementInfo
  }

}
