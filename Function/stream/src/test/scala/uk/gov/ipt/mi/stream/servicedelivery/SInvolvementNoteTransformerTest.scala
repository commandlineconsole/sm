package uk.gov.ipt.mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov.ipt.mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class SInvolvementNoteTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement" should "have involvement notes " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvementNotes = SInvolvementNoteTransformer.involvementNotes("messageId", serviceDelivery, System.currentTimeMillis())

    val expectedSize = serviceDelivery.involvements.map(_.notes.size).sum

    involvementNotes.size should equal(expectedSize)

    val firstInvolvementNote = involvementNotes.head
    firstInvolvementNote.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    firstInvolvementNote.invlmnt_handle_id should equal(serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier)

    firstInvolvementNote.rec_hash_value should not equal HashHelper.emptyHash
    firstInvolvementNote.invlmnt_hk should not equal HashHelper.emptyHash
    firstInvolvementNote.note_id should equal(serviceDelivery.involvements.sorted.head.notes.sorted.head.id.toString())
    firstInvolvementNote.note_type_cd should equal(serviceDelivery.involvements.sorted.head.notes.sorted.head.`type`.refDataValueCode)
    firstInvolvementNote.note_text should equal(serviceDelivery.involvements.sorted.head.notes.sorted.head.noteText)
  }

}
