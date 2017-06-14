package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryInvolvementElectronicAddress
import uk.gov.ipt.mi.model.servicedelivery.{ElectronicAddress, ServiceDelivery}
import uk.gov.ipt.mi.stream.HashHelper


object SInvolvementElectronicAddressTransformer {

  def involvementElectronicAddresses(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryInvolvementElectronicAddress] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)
    val invlmnt_method_cd_direct = "DIRECT"
    val invlmnt_method_cd_indirect = "INDIRECT"

    val directElectronicAddresses = serviceDelivery.involvements.sorted.flatMap(involvement => involvement.electronicAddresses.map(electronicAddressesList => {

      val invlmnt_el_addr_list_agg_hash = HashHelper.sha1(aggregateElectronicAddressesList(electronicAddressesList))

      electronicAddressesList.sorted.zipWithIndex.map {
        case (involvementElectronicAddress: ElectronicAddress, index: Int) =>

          val el_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("el_addr_handle_id", involvementElectronicAddress.internalHandle.interface_identifier)
          ))

          val rec_hash_value = HashHelper.sha1(Seq(
            ("invlmnt_el_addr_visibility", involvementElectronicAddress.internalHandle.visibility_marker.getOrElse("")),
            ("invlmnt_el_addr_start_dtime", involvementElectronicAddress.effectiveStartDate.getOrElse("")),
            ("invlmnt_el_addr_end_dtime", involvementElectronicAddress.effectiveEndDate.getOrElse("")),
            ("invlmnt_el_addr_created_by", involvementElectronicAddress.createdBy.getOrElse(""))
          ))

          val lnk_invlmnt_el_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_direct),
            ("invlmnt_el_addr_handle_id", involvementElectronicAddress.internalHandle.interface_identifier),
            ("invlmnt_el_addr_type_cd", involvementElectronicAddress.`type`.getOrElse(""))
          ))

          val invlmnt_hk = HashHelper.sha1(Seq(
            ("source",source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_direct)
          ))

          SServiceDeliveryInvolvementElectronicAddress(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
          source, serviceDelivery.internalHandle.interface_identifier, involvement.internalHandle.interface_identifier,
          invlmnt_method_cd_direct, involvementElectronicAddress.internalHandle.interface_identifier, involvementElectronicAddress.`type`,
          index, rec_hash_value, involvementElectronicAddress.internalHandle.visibility_marker,
          involvementElectronicAddress.value, involvementElectronicAddress.effectiveStartDate, involvementElectronicAddress.effectiveEndDate,
          involvementElectronicAddress.createdBy, involvementElectronicAddress.created, el_addr_hk,
          lnk_invlmnt_el_addr_hk, invlmnt_el_addr_list_agg_hash, invlmnt_hk
          )
      }


    })).flatten

    val indirectElectronicAddresses = serviceDelivery.indirectInvolvements.sorted.flatMap(indirectInvolvement => indirectInvolvement.electronicAddresses.map(electronicAddressesList => {

      val invlmnt_el_addr_list_agg_hash = HashHelper.sha1(aggregateElectronicAddressesList(electronicAddressesList))

      electronicAddressesList.sorted.zipWithIndex.map {
        case (indirectInvolvementElectronicAddress: ElectronicAddress, index: Int) =>

          val el_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("el_addr_handle_id", indirectInvolvementElectronicAddress.internalHandle.interface_identifier)
          ))

          val rec_hash_value = HashHelper.sha1(Seq(
            ("invlmnt_el_addr_visibility", indirectInvolvementElectronicAddress.internalHandle.visibility_marker.getOrElse("")),
            ("invlmnt_el_addr_start_dtime", indirectInvolvementElectronicAddress.effectiveStartDate.getOrElse("")),
            ("invlmnt_el_addr_end_dtime", indirectInvolvementElectronicAddress.effectiveEndDate.getOrElse("")),
            ("invlmnt_el_addr_created_by", indirectInvolvementElectronicAddress.createdBy.getOrElse(""))
          ))

          val lnk_invlmnt_el_addr_hk = HashHelper.sha1(Seq(
            ("record_source", source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", indirectInvolvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_direct),
            ("invlmnt_el_addr_handle_id", indirectInvolvementElectronicAddress.internalHandle.interface_identifier),
            ("invlmnt_el_addr_type_cd", indirectInvolvementElectronicAddress.`type`.getOrElse(""))
          ))

          val invlmnt_hk = HashHelper.sha1(Seq(
            ("source",source),
            ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
            ("invlmnt_handle_id", indirectInvolvement.internalHandle.interface_identifier),
            ("invlmnt_method_cd", invlmnt_method_cd_indirect)
          ))

          SServiceDeliveryInvolvementElectronicAddress(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
            source, serviceDelivery.internalHandle.interface_identifier, indirectInvolvement.internalHandle.interface_identifier,
            invlmnt_method_cd_indirect, indirectInvolvementElectronicAddress.internalHandle.interface_identifier, indirectInvolvementElectronicAddress.`type`,
            index, rec_hash_value, indirectInvolvementElectronicAddress.internalHandle.visibility_marker,
            indirectInvolvementElectronicAddress.value, indirectInvolvementElectronicAddress.effectiveStartDate, indirectInvolvementElectronicAddress.effectiveEndDate,
            indirectInvolvementElectronicAddress.createdBy, indirectInvolvementElectronicAddress.created, el_addr_hk,
            lnk_invlmnt_el_addr_hk, invlmnt_el_addr_list_agg_hash, invlmnt_hk
          )
      }
    })).flatten

    directElectronicAddresses ++ indirectElectronicAddresses
  }

  private def aggregateElectronicAddressesList(electronicAddresses: List[ElectronicAddress]): List[(String, String)] = {
    electronicAddresses.sorted.flatMap(involvementElectronicAddress =>
      List(
        ("invlmnt_el_addr_handle_id", involvementElectronicAddress.internalHandle.interface_identifier),
        ("invlmnt_el_addr_type_cd", involvementElectronicAddress.`type`.getOrElse("")),
        ("invlmnt_el_addr_visibility", involvementElectronicAddress.internalHandle.visibility_marker.getOrElse("")),
        ("invlmnt_el_addr_start_dtime", involvementElectronicAddress.effectiveStartDate.getOrElse("")),
        ("invlmnt_el_addr_end_dtime", involvementElectronicAddress.effectiveEndDate.getOrElse("")),
        ("invlmnt_el_addr_created_by", involvementElectronicAddress.createdBy.getOrElse(""))
      )
    )
  }




}
