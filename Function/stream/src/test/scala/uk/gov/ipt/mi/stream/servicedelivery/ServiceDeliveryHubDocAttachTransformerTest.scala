package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov..mi.model.HubDocAttachement
import uk.gov..mi.stream.HashHelper


@RunWith(classOf[JUnitRunner])
class ServiceDeliveryHubDocAttachTransformerTest extends FlatSpec with Matchers with Inside {

  "Service delivery Instance " should "return list of doc attachments" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val docAttachments = ServiceDeliveryHubDocAttachTransformer.hubDocAttachment("messageId", serviceDelivery, System.currentTimeMillis())

    docAttachments should have size serviceDelivery.documentAttachments.size
    inside(docAttachments.head) {

      case (HubDocAttachement(_, _, _,
      doc_atchmnt_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, doc_atchmnt_handle_id)
        ) =>
        doc_atchmnt_hk should not equal HashHelper.emptyHash
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal("")
        doc_atchmnt_handle_id should equal(serviceDelivery.documentAttachments.sorted.head.internalHandle.interface_identifier)
    }
  }

}
