package uk.gov.ipt.mi.stream.servicedelivery

import org.scalatest.{Inside, Matchers, FlatSpec}
import uk.gov.ipt.mi.model.SServiceDeliveryCorrInfo
import uk.gov.ipt.mi.stream.HashHelper


class SInvolvementDocAttachmentCorrespondenceInfoTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement document attachment correspondence  " should "should have correct info " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val correspondenceInfos: List[SServiceDeliveryCorrInfo] = SinvolvementDocAttachmentCorrespondenceInfoTransformer.correspondenceInfo("messageId", serviceDelivery, System.currentTimeMillis())

    correspondenceInfos should have size serviceDelivery.involvements.flatMap(_.documentAttachments.flatMap(_.correspondences.map(_.size))).sum

    val firstCorrespondenceInfo = correspondenceInfos.head

    firstCorrespondenceInfo.corr_rec_hash_value should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.lnk_corr_addr_hk should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.srvc_dlvry_hk should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.invlmnt_hk should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.doc_atchmnt_hk should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.corr_hk should not equal HashHelper.emptyHash
    firstCorrespondenceInfo.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    firstCorrespondenceInfo.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
    firstCorrespondenceInfo.doc_atchmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstCorrespondenceInfo.corr_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.internalHandle.interface_identifier).get)
    firstCorrespondenceInfo.corr_delivery_note should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.deliveryNote).get)
    firstCorrespondenceInfo.corr_status_cd should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.status).get)
    firstCorrespondenceInfo.po_addr_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.postalAddressId).get)
    firstCorrespondenceInfo.el_addr_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.electronicAddressId).get)

  }

}
