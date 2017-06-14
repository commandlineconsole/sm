package uk.gov..mi.stream.servicedelivery

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import uk.gov..mi.DateHelper
import uk.gov..mi.model.HubDocAttachement
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper


object SInvolvementHubDocAttachmentTransformer {

  def hubDocAttachment(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[HubDocAttachement] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.involvements.sorted.flatMap(involvement => involvement.documentAttachments.sorted.map(doc => {
      val doc_atchmnt_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
        ("doc_atchmnt_handle_id", doc.internalHandle.interface_identifier)))
      HubDocAttachement(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        doc_atchmnt_hk, source, serviceDelivery.internalHandle.interface_identifier,
        involvement.internalHandle.interface_identifier, doc.internalHandle.interface_identifier)
    }))
  }




}
