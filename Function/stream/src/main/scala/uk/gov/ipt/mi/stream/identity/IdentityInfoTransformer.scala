package uk.gov..mi.stream.identity

import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.ISODateTimeFormat
import uk.gov..mi.model.{Identity, SIdentityInfo}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper.sha1


object IdentityInfoTransformer {

  def identityInfo(messageId: String, identity: Identity, timestamp: Long): SIdentityInfo = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    val chlg_rec_hash_value = changeLogHashValue(messageId, identity)

    val vis_rec_hash_value = sha1(identity.internal_handle.visibility_marker.getOrElse(""))

    val eh_rec_hash_value = externaHandleRecHashValue(identity)

    SIdentityInfo(messageId, identity.created, fmt.print(time),
      source, identity.internal_handle.interface_identifier, chlg_rec_hash_value,
      identity.source, identity.created_by, identity.created,
      identity.auditHistory.flatMap(_.created_by), identity.auditHistory.flatMap(_.created), identity.auditHistory.flatMap(_.lastupdated_by),
      identity.auditHistory.flatMap(_.lastupdated), vis_rec_hash_value, identity.internal_handle.visibility_marker,
      eh_rec_hash_value, identity.external_handle.flatMap(_.handle_value), identity.external_handle.flatMap(_.handle_space),
      identity_hk
    )
  }

  def externaHandleRecHashValue(identity: Identity) = {
    sha1(Seq(
      ("external_handle_value", identity.external_handle.flatMap(_.handle_value).getOrElse("")),
      ("external_handle_space", identity.external_handle.flatMap(_.handle_space).getOrElse(""))))
  }

  def changeLogHashValue(messageId: String, identity: Identity) = {
    sha1(Seq(
      ("message_id", messageId),
      ("src_cd", identity.source.getOrElse("")),
      ("src_created_by", identity.created_by.getOrElse("")),
      ("aud_created_by", identity.auditHistory.flatMap(_.created_by).getOrElse("")),
      ("aud_last_updated_by", identity.auditHistory.flatMap(_.lastupdated_by).getOrElse(""))))
  }
}
