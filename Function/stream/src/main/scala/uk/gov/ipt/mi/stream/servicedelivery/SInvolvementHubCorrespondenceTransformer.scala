package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.HubCorrespondence
import uk.gov..mi.model.servicedelivery.{Correspondence, ServiceDelivery}
import uk.gov..mi.stream.HashHelper

object SInvolvementHubCorrespondenceTransformer {

  def hubCorrespondence(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[HubCorrespondence] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.involvements.sorted.flatMap(involvment => involvment.documentAttachments.sorted.flatMap(docAttach =>
      docAttach.correspondences.map(correspondences => correspondences.sorted.map((corr: Correspondence) => {

        val srvc_dlvry_handle_id = serviceDelivery.internalHandle.interface_identifier
        val invlmnt_handle_id = involvment.internalHandle.interface_identifier
        val doc_atchmnt_handle_id = docAttach.internalHandle.interface_identifier
        val corr_handle_id = corr.internalHandle.interface_identifier

        val corr_hk = HashHelper.sha1(Seq(
          ("record_source", source),
          ("srvc_dlvry_handle_id", srvc_dlvry_handle_id),
          ("invlmnt_handle_id", invlmnt_handle_id),
          ("doc_atchmnt_handle_id", doc_atchmnt_handle_id),
          ("corr_handle_id", corr_handle_id)
        ))

        HubCorrespondence(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
          corr_hk, source, srvc_dlvry_handle_id,
          invlmnt_handle_id, doc_atchmnt_handle_id, corr_handle_id)
      }
      )).getOrElse(List())))
  }

}
