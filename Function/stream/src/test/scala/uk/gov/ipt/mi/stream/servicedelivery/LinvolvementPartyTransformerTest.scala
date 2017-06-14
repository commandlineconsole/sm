package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov..mi.model.LinkServiceDeliveryInvolvementParty
import uk.gov..mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class LinvolvementPartyTransformerTest extends FlatSpec with Matchers with Inside {


  "Service Delivery Involvement" should "have link party info " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvementPartyLinks: List[LinkServiceDeliveryInvolvementParty] = LInvolvementPartyTransformer.involvementLinkParty("messageId", serviceDelivery, System.currentTimeMillis())

    involvementPartyLinks should have size serviceDelivery.involvements.size + serviceDelivery.indirectInvolvements.size

    inside(involvementPartyLinks.filter(_.invlmnt_method_cd.equals("DIRECT")).head) {
      case LinkServiceDeliveryInvolvementParty(_, _, _,
      lnk_srvcdlvry_party_invlmnt_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, _, person_handle_id,
      involved_person_id, identity_handle_id, org_handle_id,
      individual_handle_id, invlmnt_role_type_cd, invlmnt_role_subtype_cd,
      srvc_dlvry_hk, invlmnt_hk, person_hk,
      involved_person_hk, identity_hk, org_hk,
      individual_hk) =>
        lnk_srvcdlvry_party_invlmnt_hk should not equal HashHelper.emptyHash
        srvc_dlvry_hk should not equal HashHelper.emptyHash
        invlmnt_hk should not equal HashHelper.emptyHash
        person_hk should not equal HashHelper.emptyHash
        involved_person_hk should not equal HashHelper.emptyHash
        identity_hk should not equal HashHelper.emptyHash
        org_hk should not equal HashHelper.emptyHash
        individual_hk should not equal HashHelper.emptyHash

        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)
        person_handle_id should equal(serviceDelivery.involvements.sorted.head.personHandle.map(_.interface_identifier))
        individual_handle_id should equal(Some(""))
        involved_person_id should equal(serviceDelivery.involvements.sorted.head.person.map(_.personId))
        org_handle_id should equal(serviceDelivery.involvements.sorted.head.organisationHandle.map(_.interface_identifier))
        invlmnt_role_type_cd should equal(serviceDelivery.involvements.sorted.head.involvementRoleType.refDataValueCode)
        invlmnt_role_subtype_cd should equal(serviceDelivery.involvements.sorted.head.involvementRoleSubType.map(_.refDataValueCode))
    }

    inside(involvementPartyLinks.filter(_.invlmnt_method_cd.equals("INDIRECT")).head) {
      case LinkServiceDeliveryInvolvementParty(_, _, _,
      lnk_srvcdlvry_party_invlmnt_hk, _, srvc_dlvry_handle_id,
      invlmnt_handle_id, _, person_handle_id,
      involved_person_id, identity_handle_id, org_handle_id,
      individual_handle_id, invlmnt_role_type_cd, invlmnt_role_subtype_cd,
      srvc_dlvry_hk, invlmnt_hk, person_hk,
      involved_person_hk, identity_hk, org_hk,
      individual_hk) =>
        lnk_srvcdlvry_party_invlmnt_hk should not equal HashHelper.emptyHash
        srvc_dlvry_hk should not equal HashHelper.emptyHash
        invlmnt_hk should not equal HashHelper.emptyHash
        person_hk should equal(HashHelper.emptyHash)
        involved_person_hk should equal(HashHelper.emptyHash)
        identity_hk should equal(HashHelper.emptyHash)
        org_hk should not equal HashHelper.emptyHash
        individual_hk should not equal HashHelper.emptyHash

        srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
        invlmnt_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.internalHandle.interface_identifier)
        person_handle_id should equal(Some(""))
        individual_handle_id should equal(Some(serviceDelivery.indirectInvolvements.sorted.head.individualHandle.interface_identifier))
        org_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.organisationHandle.map(_.interface_identifier))
        invlmnt_role_type_cd should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementRoleType.refDataValueCode)
        invlmnt_role_subtype_cd should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementRoleSubType.map(_.refDataValueCode))
    }

  }
  

}
