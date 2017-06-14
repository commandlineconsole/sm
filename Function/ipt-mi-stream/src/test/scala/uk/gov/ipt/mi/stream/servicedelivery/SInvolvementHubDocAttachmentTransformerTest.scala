package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov.ipt.mi.model.HubDocAttachement
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class SInvolvementHubDocAttachmentTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement " should "have document attachments " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvementDocAttachmentHubs: List[HubDocAttachement] = SInvolvementHubDocAttachmentTransformer.hubDocAttachment("messageId", serviceDelivery, System.currentTimeMillis())

    involvementDocAttachmentHubs should have size serviceDelivery.involvements.map(_.documentAttachments.size).sum

    val firstDocAttachmentHub = involvementDocAttachmentHubs.head
    firstDocAttachmentHub.doc_atchmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentHub.doc_atchmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentHub.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)

  }

}
