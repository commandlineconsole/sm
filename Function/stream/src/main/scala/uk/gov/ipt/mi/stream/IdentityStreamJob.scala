package uk.gov..mi.stream

import org.apache.spark.streaming.dstream.DStream
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.model._
import uk.gov..mi.stream.HashHelper.sha1
import uk.gov..mi.stream.RDDHelper._
import uk.gov..mi.stream.identity._


class IdentityStreamJob() {

  def start(identityStream: DStream[(String, Identity, Long)], identityConfig: MIStreamIdentityConfig): Unit = {

    streamHubIdentity(identityStream, identityConfig.hubIdentityPath)
    streamSIdentityInfo(identityStream, identityConfig.sIdentityInfoPath)
    streamSIdentityBiometricInfo(identityStream, identityConfig.sIdentityBiometricInfoPath)
    streamSIdentityReference(identityStream, identityConfig.sIdentityReferencePath)
    streamSIdentityCondition(identityStream, identityConfig.sIdentityConditionPath)
    streamSIdentityBiographics(identityStream, identityConfig.sIdentityBiographicsPath)
    streamSIdentityDescrors(identityStream, identityConfig.sIdentityDescrorsPath)
    streamSIdentityMedias(identityStream, identityConfig.sIdentityMediasPath)
    streamSIdentityLinkPerson(identityStream, identityConfig.sIdentityLinkPersonPath)
  }

  def streamHubIdentity(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.map { case (messageId: String, identity: Identity, timestamp: Long) =>
      val source = ""
      val fmt = ISODateTimeFormat.dateTime()
      val time = new DateTime(timestamp, DateTimeZone.UTC)

      val identity_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("identity_handle_id", identity.internal_handle.interface_identifier))
      )

      HubIdentity(messageId, identity.created, fmt.print(time), identity_hk, source, identity.internal_handle.interface_identifier)
    }.savePartition(basePath)
  }

  def streamSIdentityInfo(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.map { case (messageId: String, identity: Identity, timestamp: Long) =>
      IdentityInfoTransformer.identityInfo(messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityCondition(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap { case (messageId: String, identity: Identity, timestamp: Long) =>
      IdentityConditionTransformer.identityCondition(messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityReference(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap { case (messageId: String, identity: Identity, timestamp: Long) =>
      IdentityReferenceTransformer.identityReference(messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityBiometricInfo(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap {case (messageId: String, identity: Identity, timestamp: Long) =>
      IdentityBiometricTransformer.identityBiometric(messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityBiographics(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap {
      case (messageId: String, identity: Identity, timestamp: Long) =>
        IdentityBiographicTransformer.identityBiographics(identity.biographic_sets, messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityDescrors(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap {
      case (messageId: String, identity: Identity, timestamp: Long) =>
        IdentityDescrionTransformer.identityDescrors(identity.descrions, messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityMedias(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap {
      case (messageId: String, identity: Identity, timestamp: Long) =>
        IdentityMediaTransformer.identityMedias(identity.identity_media_sets, messageId, identity, timestamp)
    }.savePartition(basePath)
  }

  def streamSIdentityLinkPerson(identityStream: DStream[(String, Identity, Long)], basePath: String): Unit = {
    identityStream.flatMap {
      case (messageId: String, identity: Identity, timestamp: Long) =>
        val source = ""
        val fmt = ISODateTimeFormat.dateTime()
        val time = new DateTime(timestamp, DateTimeZone.UTC)
        val identity_hk = HashHelper.sha1(Seq(
          ("record_source", source),
          ("identity_handle_id", identity.internal_handle.interface_identifier))
        )
        identity.containing_person_handle.map(personInternalHandle => {
          val lnk_idntty_person_hk = sha1(Seq(("record_source", source), ("identity_handle_id", identity.internal_handle.interface_identifier), ("person_handle_id", personInternalHandle.interface_identifier)))

          val person_hk = sha1(Seq(("record_source", source), ("person_handle_id", personInternalHandle.interface_identifier)))

          LinkIdentityPerson(messageId, identity.created, fmt.print(time),
            lnk_idntty_person_hk, source, identity.internal_handle.interface_identifier,
            personInternalHandle.interface_identifier, person_hk, identity_hk)
        })
    }.savePartition(basePath)
  }
}
