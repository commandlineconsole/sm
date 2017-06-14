package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.HubCorrespondence
import uk.gov..mi.model.servicedelivery.{Correspondence, ServiceDelivery}
import uk.gov..mi.stream.HashHelper._


object ServiceDeliveryHubCorrespondenceTransformer {

  def hubCorrespondence(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[HubCorrespondence] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.documentAttachments.sorted.flatMap(docAttach =>
      docAttach.correspondences.map(correspondences => correspondences.sorted.map((corr: Correspondence) =>
        HubCorrespondence(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
          sha1(Seq(("record_source", source))),source, serviceDelivery.internalHandle.interface_identifier,
          "", docAttach.internalHandle.interface_identifier, corr.internalHandle.interface_identifier)
      )).getOrElse(List()))
  }
}
