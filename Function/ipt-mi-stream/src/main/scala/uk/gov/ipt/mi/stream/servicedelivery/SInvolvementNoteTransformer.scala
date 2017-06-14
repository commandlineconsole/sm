package uk.gov.ipt.mi.stream.servicedelivery

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.ISODateTimeFormat
import uk.gov.ipt.mi.DateHelper
import uk.gov.ipt.mi.model.SServiceDeliveryInvolvementNote
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.HashHelper

object SInvolvementNoteTransformer {

  def involvementNotes(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryInvolvementNote] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)


    val srvc_dlvry_handle_id = serviceDelivery.internalHandle.interface_identifier
    serviceDelivery.involvements.sorted.flatMap(involvement => {

      val invlmnt_handle_id = involvement.internalHandle.interface_identifier

      involvement.notes.sorted.map(note => {
        val note_id = note.id.toString()
        val note_type_cd = note.`type`.refDataValueCode
        val note_text = note.noteText
        val rec_hash_value = HashHelper.sha1(Seq(
          ("note_id", note_id),
          ("note_type_cd", note_type_cd),
          ("note_text", note_text)
        ))

        val invlmnt_hk = HashHelper.sha1(Seq(
          ("source",source),
          ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
          ("invlmnt_handle_id", involvement.internalHandle.interface_identifier),
          ("invlmnt_method_cd", "DIRECT")
        ))

        SServiceDeliveryInvolvementNote(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
        source, srvc_dlvry_handle_id, invlmnt_handle_id,
          rec_hash_value, note_id, note_type_cd, note_text, invlmnt_hk)
      })
    })

  }
}
