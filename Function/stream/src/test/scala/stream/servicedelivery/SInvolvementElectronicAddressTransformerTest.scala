package uk.gov..mi.stream.servicedelivery

import org.scalatest.{Inside, Matchers, FlatSpec}
import uk.gov..mi.stream.HashHelper

class SInvolvementElectronicAddressTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement " should "have electronic address information" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val electronicAddresses = SInvolvementElectronicAddressTransformer.involvementElectronicAddresses("messageId", serviceDelivery, System.currentTimeMillis())

    val expectedSize = serviceDelivery.involvements.flatMap(_.electronicAddresses.map(_.size)).sum + serviceDelivery.indirectInvolvements.flatMap(_.electronicAddresses.map(_.size)).sum

    electronicAddresses.size should equal(expectedSize)

    val firstElectronicAddress = electronicAddresses.filter(_.invlmnt_method_cd.equals("DIRECT")).head
    firstElectronicAddress.el_addr_hk should not equal HashHelper.emptyHash
    firstElectronicAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstElectronicAddress.lnk_invlmnt_el_addr_hk should not equal HashHelper.emptyHash
    firstElectronicAddress.invlmnt_el_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstElectronicAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstElectronicAddress.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    firstElectronicAddress.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)

    firstElectronicAddress.invlmnt_el_addr_handle_id should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstElectronicAddress.invlmnt_el_addr_type_cd should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.`type`)
    firstElectronicAddress.invlmnt_el_addr_visibility should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.visibility_marker)
    firstElectronicAddress.invlmnt_el_addr_value should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.value)
    firstElectronicAddress.invlmnt_el_addr_start_dtime should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstElectronicAddress.invlmnt_el_addr_end_dtime should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstElectronicAddress.invlmnt_el_addr_created_by should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.createdBy)
    firstElectronicAddress.invlmnt_el_addr_created_dtime should equal(serviceDelivery.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.created)

    val firstIndirectElectronicAddress = electronicAddresses.filter(_.invlmnt_method_cd.equals("INDIRECT")).head
    firstIndirectElectronicAddress.el_addr_hk should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.lnk_invlmnt_el_addr_hk should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.invlmnt_el_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstIndirectElectronicAddress.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    firstIndirectElectronicAddress.invlmnt_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.internalHandle.interface_identifier)

    firstIndirectElectronicAddress.invlmnt_el_addr_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstIndirectElectronicAddress.invlmnt_el_addr_type_cd should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.`type`)
    firstIndirectElectronicAddress.invlmnt_el_addr_visibility should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.visibility_marker)
    firstIndirectElectronicAddress.invlmnt_el_addr_value should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.value)
    firstIndirectElectronicAddress.invlmnt_el_addr_start_dtime should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstIndirectElectronicAddress.invlmnt_el_addr_end_dtime should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstIndirectElectronicAddress.invlmnt_el_addr_created_by should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.createdBy)
    firstIndirectElectronicAddress.invlmnt_el_addr_created_dtime should equal(serviceDelivery.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.created)

  }




}
