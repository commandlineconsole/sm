package uk.gov.ipt.mi.stream.servicedelivery

import org.scalatest.{Inside, Matchers, FlatSpec}
import uk.gov.ipt.mi.stream.HashHelper

class SInvolvementPostalAddressTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement" should "have postal address information" in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val postalAddresses = SInvolvementPostalAddressTransformer.involvementPostalAddresses("messageId", serviceDelivery, System.currentTimeMillis())

    val expectedSize = serviceDelivery.involvements.flatMap(_.involvementAddresses.map(_.size)).sum + serviceDelivery.indirectInvolvements.flatMap(_.involvementAddresses.map(_.size)).sum

    postalAddresses.size should equal(expectedSize)

    val firstDirectPostalAddress = postalAddresses.filter(_.invlmnt_method_cd.equals("DIRECT")).head
    firstDirectPostalAddress.po_addr_hk should not equal HashHelper.emptyHash
    firstDirectPostalAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstDirectPostalAddress.lnk_invlmnt_po_addr_hk should not equal HashHelper.emptyHash
    firstDirectPostalAddress.invlmnt_po_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstDirectPostalAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstDirectPostalAddress.invlmnt_po_addr_handle_id should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstDirectPostalAddress.invlmnt_po_addr_usage_type_cd should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.addressUsageType.map(_.code))
    firstDirectPostalAddress.po_addr_handle_id should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.map(_.interface_identifier))
    firstDirectPostalAddress.invlmnt_po_addr_visibility should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.flatMap(_.visibility_marker))
    firstDirectPostalAddress.invlmnt_po_addr_start_dtime should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstDirectPostalAddress.invlmnt_po_addr_end_dtime should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstDirectPostalAddress.invlmnt_po_addr_created_by should equal(serviceDelivery.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.createdBy)

    val firstIndirectPostalAddress = postalAddresses.filter(_.invlmnt_method_cd.equals("INDIRECT")).head
    firstIndirectPostalAddress.po_addr_hk should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.lnk_invlmnt_po_addr_hk should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.invlmnt_po_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstIndirectPostalAddress.invlmnt_po_addr_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstIndirectPostalAddress.invlmnt_po_addr_usage_type_cd should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.addressUsageType.map(_.code))
    firstIndirectPostalAddress.po_addr_handle_id should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.map(_.interface_identifier))
    firstIndirectPostalAddress.invlmnt_po_addr_visibility should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.flatMap(_.visibility_marker))
    firstIndirectPostalAddress.invlmnt_po_addr_start_dtime should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstIndirectPostalAddress.invlmnt_po_addr_end_dtime should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstIndirectPostalAddress.invlmnt_po_addr_created_by should equal(serviceDelivery.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.createdBy)


  }

}
