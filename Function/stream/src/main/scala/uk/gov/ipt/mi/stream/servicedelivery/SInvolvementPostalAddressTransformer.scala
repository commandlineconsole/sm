package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryInvolvementPostalAddress
import uk.gov.ipt.mi.model.servicedelivery.{InvolvementAddress, ServiceDelivery}
import uk.gov.ipt.mi.stream.HashHelper

object SInvolvementPostalAddressTransformer {

  def involvementPostalAddresses(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryInvolvementPostalAddress] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)
    val invlmnt_method_cd_direct = "DIRECT"
    val invlmnt_method_cd_indirect = "INDIRECT"

    val directPOAddresses = serviceDelivery.involvements.sorted.flatMap(involvement => involvement.involvementAddresses.map(addressList => {

      val invlmnt_po_addr_list_agg_hash = HashHelper.sha1(aggregateAddressesList(addressList))

      addressList.sorted.zipWithIndex.map {
        case (involvementAddress: InvolvementAddress, index: Int) =>

          val po_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("po_addr_handle_id", involvementAddress.postalAddressHandle.map(_.interface_identifier).getOrElse("")))
          )

          val rec_hash_value = HashHelper.sha1(Seq(
            ("invlmnt_addr_visibility", involvementAddress.internalHandle.visibility_marker.getOrElse("")),
            ("invlmnt_addr_start_dtime", involvementAddress.effectiveStartDate.getOrElse("")),
            ("invlmnt_addr_end_dtime", involvementAddress.effectiveEndDate.getOrElse("")),
            ("invlmnt_addr_created_by", involvementAddress.createdBy.getOrElse(""))
          ))

          val lnk_invlmnt_po_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_direct),
            ("invlmnt_po_addr_handle_id", involvementAddress.internalHandle.interface_identifier),
            ("po_addr_handle_id", involvementAddress.postalAddressHandle.map(_.interface_identifier).getOrElse("")),
            ("invlmnt_po_addr_usage_type_cd", involvementAddress.addressUsageType.map(_.code).getOrElse(""))
          ))

          val invlmnt_hk = HashHelper.sha1(Seq(
            ("source",source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_direct)
          ))

          SServiceDeliveryInvolvementPostalAddress(messageId, involvementAddress.created, fmt.print(time),
            source, serviceDelivery.internalHandle.interface_identifier, involvement.internalHandle.interface_identifier,
            invlmnt_method_cd_direct, involvementAddress.internalHandle.interface_identifier, involvementAddress.postalAddressHandle.map(_.interface_identifier),
            involvementAddress.addressUsageType.map(_.code), index, rec_hash_value,
            involvementAddress.postalAddressHandle.flatMap(_.visibility_marker), involvementAddress.effectiveStartDate, involvementAddress.effectiveEndDate,
            involvementAddress.createdBy, involvementAddress.created, po_addr_hk,
            lnk_invlmnt_po_addr_hk, invlmnt_po_addr_list_agg_hash, invlmnt_hk
            )
      }
    })).flatten

    val indirectPOAddresses = serviceDelivery.indirectInvolvements.sorted.flatMap(indirectInvolvement => indirectInvolvement.involvementAddresses.map(addressList => {

      val indirect_invlmnt_po_addr_list_agg_hash = HashHelper.sha1(aggregateAddressesList(addressList))

      addressList.sorted.zipWithIndex.map {
        case (indirectInvolvementAddress: InvolvementAddress, index: Int) =>

          val po_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("po_addr_handle_id", indirectInvolvementAddress.postalAddressHandle.map(_.interface_identifier).getOrElse("")))
          )

          val rec_hash_value = HashHelper.sha1(Seq(
            ("invlmnt_addr_visibility", indirectInvolvementAddress.internalHandle.visibility_marker.getOrElse("")),
            ("invlmnt_addr_start_dtime", indirectInvolvementAddress.effectiveStartDate.getOrElse("")),
            ("invlmnt_addr_end_dtime", indirectInvolvementAddress.effectiveEndDate.getOrElse("")),
            ("invlmnt_addr_created_by", indirectInvolvementAddress.createdBy.getOrElse(""))
          ))

          val lnk_invlmnt_po_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", indirectInvolvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_indirect),
            ("invlmnt_po_addr_handle_id", indirectInvolvementAddress.internalHandle.interface_identifier),
            ("po_addr_handle_id", indirectInvolvementAddress.postalAddressHandle.map(_.interface_identifier).getOrElse("")),
            ("invlmnt_po_addr_usage_type_cd", indirectInvolvementAddress.addressUsageType.map(_.code).getOrElse(""))
          ))

          val invlmnt_hk = HashHelper.sha1(Seq(
            ("source",source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", indirectInvolvementAddress.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_indirect)
          ))

          SServiceDeliveryInvolvementPostalAddress(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
            source, serviceDelivery.internalHandle.interface_identifier, indirectInvolvement.internalHandle.interface_identifier,
            invlmnt_method_cd_indirect, indirectInvolvementAddress.internalHandle.interface_identifier, indirectInvolvementAddress.postalAddressHandle.map(_.interface_identifier),
            indirectInvolvementAddress.addressUsageType.map(_.code), index, rec_hash_value,
            indirectInvolvementAddress.postalAddressHandle.flatMap(_.visibility_marker), indirectInvolvementAddress.effectiveStartDate, indirectInvolvementAddress.effectiveEndDate,
            indirectInvolvementAddress.createdBy, indirectInvolvementAddress.created, po_addr_hk,
            lnk_invlmnt_po_addr_hk, indirect_invlmnt_po_addr_list_agg_hash, invlmnt_hk
          )
      }

    })).flatten

    directPOAddresses ++ indirectPOAddresses
  }

  private def aggregateAddressesList(poAddresses: List[InvolvementAddress]): List[(String, String)] = {
    poAddresses.sorted.flatMap(involvementAddress =>
      List(
        ("invlmnt_po_addr_handle_id", involvementAddress.internalHandle.interface_identifier),
        ("invlmnt_po_addr_usage_type_cd", involvementAddress.addressUsageType.map(_.code).getOrElse("")),
        ("po_addr_handle_id", involvementAddress.postalAddressHandle.map(_.interface_identifier).getOrElse("")),
        ("invlmnt_po_addr_visibility", involvementAddress.postalAddressHandle.flatMap(_.visibility_marker).getOrElse("")),
        ("invlmnt_po_addr_start_dtime", involvementAddress.effectiveStartDate.getOrElse("")),
        ("invlmnt_po_addr_end_dtime", involvementAddress.effectiveEndDate.getOrElse("")),
        ("invlmnt_po_addr_created_by", involvementAddress.createdBy.getOrElse(""))
      )
    )
  }

}
