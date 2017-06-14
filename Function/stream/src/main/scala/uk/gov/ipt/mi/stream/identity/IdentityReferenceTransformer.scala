package uk.gov.ipt.mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.model.{Identity, Reference, SIdentityReference}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper.sha1


object IdentityReferenceTransformer {

  def identityReference(messageId: String, identity: Identity, timestamp: Long): List[SIdentityReference] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val collectionStr = referenceAggStr(identity)

    val reference_list_agg_hash = sha1(collectionStr)
    val lnk_idntty_reference_list_hk = sha1(Seq(("record_source", source), ("identity_handle_id", identity.internal_handle.interface_identifier)) ++ collectionStr)

    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    identity.references.getOrElse(Set()).toList.sorted.zipWithIndex.map { case (reference: Reference, i: Int) =>
      val rec_hash_value = sha1(referenceRecStr(reference))

      SIdentityReference(messageId, identity.created, fmt.print(time),
        source, identity.internal_handle.interface_identifier, reference.internalHandle.interface_identifier,
        i, rec_hash_value, reference.internalHandle.visibility_marker,
        reference.externalHandle.flatMap(_.handle_value), reference.externalHandle.flatMap(_.handle_space), reference.referenceType.flatMap(_.code),
        reference.referenceValue, reference.createdBy, reference.created,
        reference_list_agg_hash, lnk_idntty_reference_list_hk, identity_hk
      )
    }
  }

  def referenceAggStr(identity: Identity) = {
    identity.references.map { case references: Set[Reference] =>
      references.toList.sorted.flatMap(referenceRecStr(_))
    }.getOrElse(List())
  }

  def referenceRecStr(reference: Reference): List[(String, String)] = {
    List(
      ("reference_visibility", reference.internalHandle.visibility_marker.getOrElse("")),
      ("reference_ext_handle_value", reference.externalHandle.flatMap(_.handle_value).getOrElse("")),
      ("reference_ext_handle_space", reference.externalHandle.flatMap(_.handle_space).getOrElse("")),
      ("reference_type_cd", reference.referenceType.flatMap(_.code).getOrElse("")),
      ("reference_value", reference.referenceValue.getOrElse("")),
      ("reference_created_by", reference.createdBy.getOrElse(""))
    )
  }
}
