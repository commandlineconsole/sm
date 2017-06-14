package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov.ipt.mi.model.SServiceDeliveryDocAttachmentInfo
import uk.gov.ipt.mi.model.servicedelivery.DocumentAttachment
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class ServiceDeliveryDocAttachmentInfoTest extends FlatSpec with Matchers with Inside {

  "Service delivery Instance " should "return list of doc attachment infos" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val docAttachmentInfos = ServiceDeliveryDocAttachmentInfo.docAttachmentInfos("messageId", serviceDelivery, System.currentTimeMillis())

    docAttachmentInfos should have size serviceDelivery.documentAttachments.size

    inside(docAttachmentInfos.head) {
      case SServiceDeliveryDocAttachmentInfo(_, _, _,
      _, srvc_dlvry_handle_id, invlmnt_handle_id,
      doc_atchmnt_handle_id, main_rec_hash_value, doc_atchmnt_handle_visibility,
      doc_atchmnt_type_cd, doc_atchmnt_external_ref, doc_atchmnt_desc,
      doc_atchmnt_doc_store_id, doc_atchmnt_mime_type_cd, doc_atchmnt_record_datetime,
      doc_atchmnt_provider_cd, doc_atchmnt_verified_flag, doc_atchmnt_stage_cd,
      chlg_rec_hash_value, doc_atchmnt_created_by, doc_atchmnt_created_datetime,
      doc_atchmnt_last_updated_by, doc_atchmnt_last_updated_dtime, srvc_dlvry_hk,
      invlmnt_hk, doc_atchmnt_hk) =>
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal("")
        val expectedDoc: DocumentAttachment = serviceDelivery.documentAttachments.sorted.head
        doc_atchmnt_handle_id should equal(expectedDoc.internalHandle.interface_identifier)
        main_rec_hash_value should not equal HashHelper.emptyHash
        doc_atchmnt_handle_visibility should equal(expectedDoc.internalHandle.visibility_marker)
        doc_atchmnt_type_cd should equal(expectedDoc.attachmentType.refDataValueCode)
        doc_atchmnt_external_ref should equal(expectedDoc.externalReference)
        doc_atchmnt_desc should equal(expectedDoc.description)
        doc_atchmnt_doc_store_id should equal(expectedDoc.documentStoreId)
        doc_atchmnt_mime_type_cd should equal(expectedDoc.mimeType.refDataValueCode)
        doc_atchmnt_record_datetime should equal(expectedDoc.recordDate)
        doc_atchmnt_provider_cd should equal(expectedDoc.provider.refDataValueCode)
        doc_atchmnt_verified_flag should equal(expectedDoc.verified)
        doc_atchmnt_stage_cd should equal(expectedDoc.stageCode)
        chlg_rec_hash_value should not equal HashHelper.emptyHash
        doc_atchmnt_created_by should equal(expectedDoc.createdBy)
        doc_atchmnt_created_datetime should equal(expectedDoc.created)
        doc_atchmnt_last_updated_by should equal(expectedDoc.lastUpdatedBy)
        doc_atchmnt_last_updated_dtime should equal(expectedDoc.lastUpdated)
        srvc_dlvry_hk should not equal HashHelper.emptyHash
        invlmnt_hk should equal(HashHelper.emptyHash)
        doc_atchmnt_hk should equal(HashHelper.emptyHash)
    }
  }

}
