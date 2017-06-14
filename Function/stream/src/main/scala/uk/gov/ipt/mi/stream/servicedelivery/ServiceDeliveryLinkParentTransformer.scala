package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.LinkServiceDeliveryParent
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._

object ServiceDeliveryLinkParentTransformer {

  def linkParent(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): Option[LinkServiceDeliveryParent] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))

    serviceDelivery.parentServiceDelivery.map(parentServiceDelivery => {
      val lnk_srvc_dlvry_parent_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("parent_srvc_dlvry_handle_id", parentServiceDelivery)))

      val parent_srvc_dlvry_hk = sha1(Seq(("record_source", source), ("parent_srvc_dlvry_handle_id", parentServiceDelivery)))

      LinkServiceDeliveryParent(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        lnk_srvc_dlvry_parent_hk, source, serviceDelivery.internalHandle.interface_identifier,
        serviceDelivery.parentServiceDelivery, srvc_dlvry_hk, parent_srvc_dlvry_hk)
    })
  }
}
