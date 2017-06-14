package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.HubServiceDelivery
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper._


object ServiceDeliveryHubTransformer {

  def serviceDeliveryHub(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): HubServiceDelivery = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))
    HubServiceDelivery(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time), srvc_dlvry_hk, source, serviceDelivery.internalHandle.interface_identifier)
  }
}
