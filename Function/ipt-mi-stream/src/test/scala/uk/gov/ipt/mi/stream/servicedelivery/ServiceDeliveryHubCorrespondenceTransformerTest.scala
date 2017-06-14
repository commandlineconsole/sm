package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov.ipt.mi.model.HubCorrespondence
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class ServiceDeliveryHubCorrespondenceTransformerTest extends FlatSpec with Matchers with Inside {

  "Service delivery attributes " should "return correct Attribute list" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val correspondences = ServiceDeliveryHubCorrespondenceTransformer.hubCorrespondence("messageId", serviceDelivery, System.currentTimeMillis())

    correspondences should have size serviceDelivery.documentAttachments.flatMap(_.correspondences.map(_.size)).sum

    inside(correspondences.head) {
      case HubCorrespondence(_, _, _,
      corr_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, doc_atchmnt_handle_id, corr_handle_id) =>
        corr_hk should not equal HashHelper.emptyHash
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal("")
        doc_atchmnt_handle_id should equal(serviceDelivery.documentAttachments.sorted.head.internalHandle.interface_identifier)
        Some(corr_handle_id) should equal(serviceDelivery.documentAttachments.sorted.head.correspondences.map(_.sorted.head.internalHandle.interface_identifier))
    }
  }
}
