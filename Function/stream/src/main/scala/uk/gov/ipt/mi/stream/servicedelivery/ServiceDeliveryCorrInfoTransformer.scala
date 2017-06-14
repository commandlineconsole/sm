package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.SServiceDeliveryCorrInfo
import uk.gov..mi.model.servicedelivery.{Correspondence, ServiceDelivery}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._


object ServiceDeliveryCorrInfoTransformer {

  def correspondenceInfo(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryCorrInfo] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.documentAttachments.sorted.flatMap(docAttach =>
      docAttach.correspondences.map(correspondences => correspondences.sorted.map((corr: Correspondence) => {
        val corr_rec_hash_value = HashHelper.sha1(Seq(
          ("corr_handle_visibility", corr.internalHandle.visibility_marker.getOrElse("")),
          ("corr_delivery_note", corr.deliveryNote.getOrElse("")),
          ("corr_created_by", corr.createdBy.getOrElse("")),
          ("corr_created_datetime", corr.created.getOrElse("")),
          ("corr_last_updated_by", corr.lastUpdatedBy.getOrElse("")),
          ("corr_last_updated_datetime", corr.lastUpdated.getOrElse("")),
          ("corr_status_cd", corr.status.getOrElse(""))
        ))

        val lnk_corr_addr_hk = HashHelper.sha1(Seq(
          ("record_source", source),
          ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
          ("doc_atchmnt_handle_id", docAttach.internalHandle.interface_identifier),
          ("corr_handle_id", corr.internalHandle.interface_identifier),
          ("po_addr_handle_id", corr.postalAddressId.getOrElse("")),
          ("el_add_handle_id", corr.electronicAddressId.getOrElse(""))
        ))

        val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))
        val invlmnt_hk = HashHelper.emptyHash
        val doc_atchmnt_hk = HashHelper.emptyHash
        val corr_hk = HashHelper.emptyHash



        SServiceDeliveryCorrInfo(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
          source, serviceDelivery.internalHandle.interface_identifier, "",
          docAttach.internalHandle.interface_identifier, corr.internalHandle.interface_identifier, corr_rec_hash_value,
          corr.internalHandle.visibility_marker, corr.deliveryNote, corr.createdBy,
          corr.created, corr.lastUpdatedBy, corr.lastUpdated,
          corr.status, corr.postalAddressId, corr.electronicAddressId, lnk_corr_addr_hk,
          srvc_dlvry_hk, invlmnt_hk, doc_atchmnt_hk,
          corr_hk)
      }
      )).getOrElse(List()))
  }

}
