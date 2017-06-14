package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.HubDocAttachement
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.HashHelper


object ServiceDeliveryHubDocAttachTransformer {

  def hubDocAttachment(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[HubDocAttachement] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.documentAttachments.sorted.map(doc => {
      val doc_atchmnt_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", ""),
        ("doc_atchmnt_handle_id", doc.internalHandle.interface_identifier)))

      HubDocAttachement(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        doc_atchmnt_hk, source, serviceDelivery.internalHandle.interface_identifier,
        "", doc.internalHandle.interface_identifier)
    })
  }

}
