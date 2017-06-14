package uk.gov.ipt.mi.stream

import org.apache.spark.streaming.dstream.DStream
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.model._
import uk.gov.ipt.mi.stream.HashHelper.sha1
import uk.gov.ipt.mi.stream.RDDHelper._

class PersonStreamJob() {

  def start(personStream: DStream[(String, Person, Long)], personConfig: MiStreamPersonConfig): Unit = {
    streamHubPerson(personStream, personConfig.hubPersonPath)

    streamPersonChangeLog(personStream, personConfig.personChangeLogPath)

    streamPersonVisibility(personStream, personConfig.personVisibilityPath)

    streamLinkPerson(personStream, personConfig.linkPersonPath)
  }

  def streamHubPerson(personStreamTime: DStream[(String, Person, Long)], basePath: String): Unit = {
    val hubPerson = personStreamTime.map { case (messageId: String, person: Person, timestamp: Long) =>
      val source = "IPT"
      val fmt = ISODateTimeFormat.dateTime()
      val time = new DateTime(timestamp, DateTimeZone.UTC)

      val person_hk = sha1(source + person.internal_handle.interface_identifier)

      HubPerson(messageId, person.created, fmt.print(time), person_hk, source, person.internal_handle.interface_identifier)
    }
    hubPerson.savePartition(basePath)
  }

  def streamPersonChangeLog(personStreamTime: DStream[(String, Person, Long)], basePath: String): Unit = {
    personStreamTime.map { case (messageId: String, person: Person, timestamp: Long) =>
      val source = "IPT"
      val fmt = ISODateTimeFormat.dateTime()
      val time = new DateTime(timestamp, DateTimeZone.UTC)

      val person_hk = sha1(source + person.internal_handle.interface_identifier)

      val rec_hash_value = sha1(Seq(("message_id", messageId), ("person_space", person.person_space.getOrElse("")),
        ("src_created_by", person.created_by.getOrElse("")),
        ("src_created_datetime", person.created.getOrElse(""))))

      PersonChangeLog(messageId, person.created, fmt.print(time),
        source, person.internal_handle.interface_identifier, rec_hash_value,
        person.person_space, person.created_by, person.created, person_hk)
    }.savePartition(basePath)
  }

  def streamPersonVisibility(personStreamTime: DStream[(String, Person, Long)], basePath: String): Unit = {
    personStreamTime.map { case (messageId: String, person: Person, timestamp: Long) =>
      val source = "IPT"
      val fmt = ISODateTimeFormat.dateTime()
      val time = new DateTime(timestamp, DateTimeZone.UTC)

      val person_hk = sha1(source + person.internal_handle.interface_identifier)

      val rec_hash_value = sha1(Seq(("person_handle_visibility", person.internal_handle.visibility_marker.getOrElse(""))))

      PersonVisibility(messageId, person.created, fmt.print(time),
        source, person.internal_handle.interface_identifier, rec_hash_value,
        person.internal_handle.visibility_marker, person_hk)
    }.savePartition(basePath)
  }

  def streamLinkPerson(personStreamTime: DStream[(String, Person, Long)], basePath: String): Unit = {
    personStreamTime.flatMap { case (messageId: String, person: Person, timestamp: Long) =>
      val source = "IPT"
      val fmt = ISODateTimeFormat.dateTime()
      val time = new DateTime(timestamp, DateTimeZone.UTC)

      val person_hk = sha1(source + person.internal_handle.interface_identifier)

      person.identity_handles.map(identityHandle => {
        val lnk_person_idntty_hk = sha1(Seq(("record_source", source),
          ("person_handle_id", person.internal_handle.interface_identifier),
          ("identity_handle_id", identityHandle.interface_identifier)))

        val identity_hk = HashHelper.sha1(Seq(
          ("record_source", source),
          ("identity_handle_id", identityHandle.interface_identifier))
        )
        LinkPerson(messageId, person.created, fmt.print(time),
          lnk_person_idntty_hk, source, person.internal_handle.interface_identifier,
          identityHandle.interface_identifier, person_hk, identity_hk)
      })
    }.savePartition(basePath)
  }
}
