package uk.gov..mi.stream.servicedelivery

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import uk.gov..mi.DateHelper
import uk.gov..mi.model.HubServiceDeliveryInvolvement
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper


object InvolvementHubTransformer {
  def involvementHub(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) : List[HubServiceDeliveryInvolvement] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val directInvolvement: List[HubServiceDeliveryInvolvement] = serviceDelivery.involvements.sorted.map(involvement => {
      val invlmnt_method_cd = "DIRECT"
      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))
      HubServiceDeliveryInvolvement(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        invlmnt_hk, source, serviceDelivery.internalHandle.interface_identifier,
        involvement.internalHandle.interface_identifier, invlmnt_method_cd)
    })

    val inDirectInvolvement: List[HubServiceDeliveryInvolvement] = serviceDelivery.indirectInvolvements.sorted.map(indInvolvement => {
      val invlmnt_method_cd = "INDIRECT"
      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", indInvolvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))
      HubServiceDeliveryInvolvement(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        invlmnt_hk, source, serviceDelivery.internalHandle.interface_identifier,
        indInvolvement.internalHandle.interface_identifier, invlmnt_method_cd)
    })

    directInvolvement ++ inDirectInvolvement
  }

}
