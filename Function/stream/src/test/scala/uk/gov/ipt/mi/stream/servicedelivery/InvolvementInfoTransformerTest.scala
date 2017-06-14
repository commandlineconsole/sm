package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Inside, Matchers, FlatSpec}
import uk.gov..mi.model.SServiceDeliveryInvolvementInfo
import uk.gov..mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class InvolvementInfoTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement" should "have direct involvement infos " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvementInfos: List[SServiceDeliveryInvolvementInfo] = SInvolvementInfoTransformer.involvementInfo("messageId", serviceDelivery, System.currentTimeMillis())

    involvementInfos should have size serviceDelivery.involvements.size + serviceDelivery.indirectInvolvements.size

    inside(involvementInfos.filter(_.invlmnt_method_cd.equals("DIRECT")).head) {
      case SServiceDeliveryInvolvementInfo(_, _, _,
      _, srvc_dlvry_handle_id, invlmnt_handle_id,
      _, chlog_rec_hash_value, invlmnt_hk,
      _, _, _,
      _, vis_rec_hash_value, invlmnt_handle_visibility, _) =>
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
        chlog_rec_hash_value should not equal HashHelper.emptyHash
        vis_rec_hash_value should not equal HashHelper.emptyHash
        invlmnt_hk should not equal HashHelper.emptyHash
        invlmnt_handle_visibility should equal(serviceDelivery.involvements.sorted.head.internalHandle.visibility_marker)
    }

    inside(involvementInfos.filter(_.invlmnt_method_cd.equals("INDIRECT")).head) {
      case SServiceDeliveryInvolvementInfo(_, _, _,
      _, srvc_dlvry_handle_id, invlmnt_handle_id,
      _, chlog_rec_hash_value, _,
      _, _, _,
      _, vis_rec_hash_value, invlmnt_handle_visibility, invlmnt_hk) =>
        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.internalHandle.interface_identifier)
        chlog_rec_hash_value should not equal HashHelper.emptyHash
        vis_rec_hash_value should not equal HashHelper.emptyHash
        invlmnt_hk should not equal HashHelper.emptyHash
        invlmnt_handle_visibility should equal(serviceDelivery.indirectInvolvements.sorted.head.internalHandle.visibility_marker)
    }


  }


}
