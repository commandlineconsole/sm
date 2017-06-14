package uk.gov.ipt.mi.stream.servicedelivery

import org.scalatest.{Inside, Matchers, FlatSpec}
import uk.gov.ipt.mi.model.HubCorrespondence
import uk.gov.ipt.mi.stream.HashHelper


class SInvolvementHubCorrespondenceTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement document attachment  " should "should have correct correspondence " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val correspondences: List[HubCorrespondence] = SInvolvementHubCorrespondenceTransformer.hubCorrespondence("messageId", serviceDelivery, System.currentTimeMillis())

    correspondences should have size serviceDelivery.involvements.flatMap(_.documentAttachments.flatMap(_.correspondences.map(_.size))).sum

    val firstCorrespondence = correspondences.head

    firstCorrespondence.corr_hk should not equal HashHelper.emptyHash
    firstCorrespondence.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    firstCorrespondence.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
    firstCorrespondence.doc_atchmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstCorrespondence.corr_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.internalHandle.interface_identifier).get)

  }

}
