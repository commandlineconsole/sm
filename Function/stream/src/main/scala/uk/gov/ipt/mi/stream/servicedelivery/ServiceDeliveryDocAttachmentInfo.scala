package uk.gov..mi.stream.servicedelivery

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import uk.gov..mi.DateHelper
import uk.gov..mi.model.SServiceDeliveryDocAttachmentInfo
import uk.gov..mi.model.servicedelivery.{DocumentAttachment, ServiceDelivery}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._


object ServiceDeliveryDocAttachmentInfo {

  def docAttachmentInfos(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryDocAttachmentInfo] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.documentAttachments.sorted.map(docAttachment => {
      val main_rec_hash_value = HashHelper.sha1(docAttachmentStr(docAttachment))
      val chlg_rec_hash_value = HashHelper.sha1(Seq(
        ("message_id", messageId),
        ("doc_created_by", docAttachment.createdBy.getOrElse("")),
        ("doc_last_updated_by", docAttachment.lastUpdatedBy.getOrElse(""))))

      val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))
      val invlmnt_hk = HashHelper.emptyHash
      val doc_atchmnt_hk = HashHelper.emptyHash

      SServiceDeliveryDocAttachmentInfo(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        source, serviceDelivery.internalHandle.interface_identifier, "",
        docAttachment.internalHandle.interface_identifier, main_rec_hash_value, docAttachment.internalHandle.visibility_marker,
        docAttachment.attachmentType.refDataValueCode, docAttachment.externalReference, docAttachment.descrion,
        docAttachment.documentStoreId, docAttachment.mimeType.refDataValueCode, docAttachment.recordDate,
        docAttachment.provider.refDataValueCode, docAttachment.verified, docAttachment.stageCode,
        chlg_rec_hash_value, docAttachment.createdBy, docAttachment.created,
        docAttachment.lastUpdatedBy, docAttachment.lastUpdated,
        srvc_dlvry_hk, invlmnt_hk, doc_atchmnt_hk
      )
    })
  }

  private def docAttachmentStr(documentAttachment: DocumentAttachment): List[(String, String)]= {
    List(
      ("doc_atchmnt_handle_visibility", documentAttachment.internalHandle.visibility_marker.getOrElse("")),
      ("doc_atchmnt_type_cd", documentAttachment.attachmentType.refDataValueCode),
      ("doc_atchmnt_external_ref", documentAttachment.externalReference),
      ("doc_atchmnt_doc_store_id", documentAttachment.documentStoreId),
      ("doc_atchmnt_mime_type_cd", documentAttachment.mimeType.refDataValueCode),
      ("doc_atchmnt_record_datetime", documentAttachment.created.getOrElse("")),
      ("doc_atchmnt_provider_cd", documentAttachment.provider.refDataValueCode),
      ("doc_atchmnt_verified_flag", documentAttachment.verified.toString),
      ("doc_atchmnt_stage_cd", documentAttachment.stageCode)
    )
  }
}
