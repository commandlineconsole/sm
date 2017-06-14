package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov.ipt.mi.model.SServiceDeliveryDocAttachmentInfo
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class SInvolvementDocAttachmentInfoTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement Doc Attachment" should "have have correct info " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val docAttachmentInfoSatellites: List[SServiceDeliveryDocAttachmentInfo] = SInvolvementDocAttachmentInfoTransformer.docAttachment("messageId", serviceDelivery, System.currentTimeMillis())
    docAttachmentInfoSatellites should have size serviceDelivery.involvements.map(_.documentAttachments.size).sum

    val firstDocAttachmentInfoSatellite = docAttachmentInfoSatellites.head
    firstDocAttachmentInfoSatellite.main_rec_hash_value should not equal HashHelper.emptyHash
    firstDocAttachmentInfoSatellite.srvc_dlvry_hk should not equal HashHelper.emptyHash
    firstDocAttachmentInfoSatellite.invlmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentInfoSatellite.doc_atchmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentInfoSatellite.chlg_rec_hash_value should not equal HashHelper.emptyHash
    firstDocAttachmentInfoSatellite.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentInfoSatellite.doc_atchmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentInfoSatellite.doc_atchmnt_doc_store_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.documentStoreId)
    firstDocAttachmentInfoSatellite.doc_atchmnt_mime_type_cd should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.mimeType.refDataValueCode)
    firstDocAttachmentInfoSatellite.doc_atchmnt_provider_cd should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.provider.refDataValueCode)
    firstDocAttachmentInfoSatellite.doc_atchmnt_verified_flag should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.verified)
    firstDocAttachmentInfoSatellite.doc_atchmnt_stage_cd should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.stageCode)



  }

}
