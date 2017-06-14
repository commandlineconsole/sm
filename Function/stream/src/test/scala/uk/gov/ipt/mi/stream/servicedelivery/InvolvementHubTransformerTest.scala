package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov.ipt.mi.model.HubServiceDeliveryInvolvement
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class InvolvementHubTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement " should "have direct and indirect involvement " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvements: List[HubServiceDeliveryInvolvement] = InvolvementHubTransformer.involvementHub("messageId", serviceDelivery, System.currentTimeMillis())


    involvements should have size serviceDelivery.involvements.size + serviceDelivery.indirectInvolvements.size

    inside(involvements.filter(_.invlmnt_method_cd.equals("DIRECT")).head) {
      case HubServiceDeliveryInvolvement(_, _, _,
      invlmnt_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, invlmnt_method_cd) =>
        invlmnt_hk should not equal HashHelper.emptyHash
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
        invlmnt_method_cd should equal("DIRECT")
    }

    inside(involvements.filter(_.invlmnt_method_cd.equals("INDIRECT")).head) {
      case HubServiceDeliveryInvolvement(_, _, _,
      invlmnt_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, invlmnt_method_cd) =>
        invlmnt_hk should not equal HashHelper.emptyHash
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.internalHandle.interface_identifier)
        invlmnt_method_cd should equal("INDIRECT")
    }
  }

}
