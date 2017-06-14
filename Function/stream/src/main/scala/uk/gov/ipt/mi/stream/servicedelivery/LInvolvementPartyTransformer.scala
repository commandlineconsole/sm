package uk.gov..mi.stream.servicedelivery

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import uk.gov..mi.DateHelper
import uk.gov..mi.model.LinkServiceDeliveryInvolvementParty
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._


object LInvolvementPartyTransformer {

  def involvementLinkParty(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long) : List[LinkServiceDeliveryInvolvementParty] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val srvc_dlvry_hk = sha1(Seq(("record_source", source), ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier)))

    val directInvolvementResult = serviceDelivery.involvements.sorted.map(involvement => {
      val invlmnt_method_cd = "DIRECT"
      val srvc_dlvry_handle_id = serviceDelivery.internalHandle.interface_identifier
      val invlmnt_handle_id = involvement.internalHandle.interface_identifier
      val person_handle_id = involvement.personHandle.map(_.interface_identifier)
      val individual_handle_id = Some("") //not applicable for direct involvements
      val involved_person_id = involvement.person.map(_.personId)
      val org_handle_id = involvement.organisationHandle.map(_.interface_identifier)
      val invlmnt_role_type_cd = involvement.involvementRoleType.refDataValueCode
      val invlmnt_role_subtype_cd = involvement.involvementRoleSubType.map(_.refDataValueCode)

      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))

      val involved_person_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("involved_person_id", involvement.person.map(_.personId).getOrElse(""))
      ))

      val person_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("person_handle_id", person_handle_id.getOrElse(""))
      ))

      val identity_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("identity_handle_id", involvement.person.flatMap(_.defaultBiographics.map(_.identityHandle.interface_identifier)).getOrElse(""))
      ))

      val org_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("org_handle_id", org_handle_id.getOrElse(""))
      ))

      val individual_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("individual_handle_id", individual_handle_id.getOrElse(""))
      ))

      val lnk_srvcdlvry_party_invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", srvc_dlvry_handle_id),
        ("invlmnt_handle_id", invlmnt_handle_id),
        ("invlmnt_method_cd", invlmnt_method_cd),
        ("person_handle_id", person_handle_id.getOrElse("")),
        ("individual_handle_id", individual_handle_id.getOrElse("")),
        ("org_handle_id", org_handle_id.getOrElse(""))
      ))

      LinkServiceDeliveryInvolvementParty(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        lnk_srvcdlvry_party_invlmnt_hk, source, srvc_dlvry_handle_id,
        invlmnt_handle_id, invlmnt_method_cd, person_handle_id,
        involved_person_id, involvement.person.flatMap(_.defaultBiographics.map(_.identityHandle.interface_identifier)), org_handle_id,
        individual_handle_id, invlmnt_role_type_cd, invlmnt_role_subtype_cd,
        srvc_dlvry_hk, invlmnt_hk, person_hk, involved_person_hk,
        identity_hk, org_hk, individual_hk
        )
    })

    val indirectInvolvementResult = serviceDelivery.indirectInvolvements.sorted.map(indirectInvolvement => {
      val invlmnt_method_cd = "INDIRECT"
      val srvc_dlvry_handle_id = serviceDelivery.internalHandle.interface_identifier
      val invlmnt_handle_id = indirectInvolvement.internalHandle.interface_identifier
      val person_handle_id = Some("")
      val individual_handle_id = Some(indirectInvolvement.individualHandle.interface_identifier)
      val involved_person_id = Some("") //not applicable for indirect involvements

      val org_handle_id = indirectInvolvement.organisationHandle.map(_.interface_identifier)
      val invlmnt_role_type_cd = indirectInvolvement.involvementRoleType.refDataValueCode
      val invlmnt_role_subtype_cd = indirectInvolvement.involvementRoleSubType.map(_.refDataValueCode)

      val invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("invlmnt_handle_id", indirectInvolvement.internalHandle.interface_identifier),
        ("invlmnt_method_cd", invlmnt_method_cd)
      ))

      val involved_person_hk = HashHelper.emptyHash
      val person_hk = HashHelper.emptyHash
      val identity_hk = HashHelper.emptyHash

      val org_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("org_handle_id", org_handle_id.getOrElse(""))
      ))

      val individual_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("individual_handle_id", individual_handle_id.getOrElse(""))
      ))

      val lnk_srvcdlvry_party_invlmnt_hk = HashHelper.sha1(Seq(
        ("source",source),
        ("srvc_dlvry_handle_id", srvc_dlvry_handle_id),
        ("invlmnt_handle_id", invlmnt_handle_id),
        ("invlmnt_method_cd", invlmnt_method_cd),
        ("person_handle_id", person_handle_id.getOrElse("")),
        ("org_handle_id", org_handle_id.getOrElse(""))
      ))

      LinkServiceDeliveryInvolvementParty(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        lnk_srvcdlvry_party_invlmnt_hk, source, srvc_dlvry_handle_id,
        invlmnt_handle_id, invlmnt_method_cd, person_handle_id,
        involved_person_id, Some(""), org_handle_id,
        individual_handle_id, invlmnt_role_type_cd, invlmnt_role_subtype_cd,
        srvc_dlvry_hk, invlmnt_hk, person_hk, involved_person_hk,
        identity_hk, org_hk, individual_hk
      )

    })

    directInvolvementResult ++ indirectInvolvementResult
  }

}
