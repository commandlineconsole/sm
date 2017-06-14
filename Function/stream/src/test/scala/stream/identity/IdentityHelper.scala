package uk.gov..mi.stream.identity

import uk.gov..mi.model._
import uk.gov..mi.stream.ModelHelper._


object IdentityHelper {


  def createIdentity(ihId: String, ehId: String) = {
    Identity(internalHandle(ihId), Some(externalHandle(ehId)), Some("created"), Some("createdBy"),
      Some(Set(biographicSet("biographicSetId"))), Some(biometrics), Some(descions), Some(references), Some(conditions), Some(Set(mediaSet(s"$ihId-mediaset"))), Some(internalHandle("personId")), Some("source"), None)
  }

  def basicIdentity() = {
    Identity(internalHandle("basic-id"), None, None, None, None, None, None, None, None, None, None, None, None)
  }

  def descions(): Set[Descrion] = Set(descrion("descr-id"))

  def descrion(id: String) = Descrion(internalHandle(s"$id-descrion"), Some(externalHandle(s"$id-descrion")),
    Some("createdTime"), Some("createdBy"), Some(descrors()))

  def descrors(): Set[Descror] = Set(
    descror("id1", "type1", "value1"),
    descror("id2", "type2", "value2")
  )

  def descror(id: String, desc_type: String, value: String) = Descror(internalHandle(s"$id-descror"), Some(desc_type), Some(value), Some("createdDate"), Some("created_by"))

  def biographic(id: String) = Biographic(internalHandle(s"$id-biog"), Some(s"$id-TYPE"), Some("Biographic-Value"), Some("Biographic-Value-Type"),
    Some("created"), Some("createdBy"), Some("referenceDataSet"))

  def biographicSet(id: String) = BiographicSet(internalHandle(s"$id-internal"), Some(externalHandle(s"$id-external")),
    Some(ReferenceType(Some("code"), None, None)), Some("created"), Some("createdBy"), Map(("GIVEN_NAME", biographic("GIVEN_NAME")), ("GENDER", biographic("GENDER"))))

  def conditions(): Set[IdentityCondition] = Set(
    condition("LOCALALERT"),
    condition("GLOBALALERT")
  )

  def condition(note: String) = IdentityCondition(internalHandle(note), Some(externalHandle(note)), Some(ReferenceType(Some(note), Some(s"$note-short"), Some(s"$note-long"))),
    Some("conditionStartDate"), Some("conditionUpdateDate"), Some(note), Some("conditionEndDate"), None, None, None)

  def references(): Set[Reference] = Set(
    reference("PASSPORT", "1233241234234"),
    reference("NATIONAL_ID", "000999111")
  )

  def reference(code: String, value: String) = Reference(internalHandle(code), Some(externalHandle(code)), Some(ReferenceType(Some(code), Some(s"$code-short"), Some(s"$code-long"))), Some(value), None, None)

  def biometrics(): Set[Biometric] = Set(
    biometric("FACE", "UERN", "811415192", externalHandle("811415192_UERN"), internalHandle("IABS_BIOMETRIC_811415192_UERN")),
    biometric("FACE", "RECORD_EVENT_REFERENCE", "ARQ15/001608/Z5P11_11415192", externalHandle("ARQ15/001608/Z5P11_11415192_RECORD_EVENT_REFERENCE"), internalHandle("IABS_BIOMETRIC_ARQ15/001608/Z5P11_11415192_RECORD_EVENT_REFERENCE"))
  )

  def biometric(nature: String, reference: String, value: String, externalHandle: ExternalHandle, internalHandle: InternalHandle): Biometric =
    Biometric(internalHandle, Some(externalHandle), Some(reference), Some(nature), Some(value), None, None)

  def mediaSet(id: String) = MediaSet(internalHandle(s"$id-ih-mediaset"), Some(externalHandle(s"$id-ex-mediaseet")), Some("mediaSetCreated"), None,
    Some(Set(media("media1"), media("media2")))
  )

  def media(id: String) = IdentityMedia(internalHandle(s"$id-internal"), Some(s"$id-media-type"), Some(internalHandle(s"$id-filehandle")), Some("created-media"), Some("createdBy-media"))

}
