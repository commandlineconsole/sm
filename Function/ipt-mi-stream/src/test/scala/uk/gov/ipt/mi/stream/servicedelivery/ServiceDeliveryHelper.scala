package uk.gov.ipt.mi.stream.servicedelivery

import uk.gov.ipt.mi.model.servicedelivery._
import uk.gov.ipt.mi.stream.ModelHelper._


object ServiceDeliveryHelper {

  val random = scala.util.Random

  def biographicSet(id: String) : Set[Biographic] = {
    Set(
      Biographic(Some(internalHandle(id + "1")), s"$id-1-type", s"$id-1-value"),
      Biographic(Some(internalHandle(id + "2")), s"$id-2-type", s"$id-2-value")
    )
  }

  def referenceData() = {
    val id = random.nextString(3)
    ReferenceData(id, s"$id-short", s"$id-long")
  }

  def referenceDataElement() = {
    val id = random.nextLong
    ReferenceDataElement(s"$id-code", id)
  }

  def defaultBiographics(id: String) = DefaultBiographics(internalHandle(id), referenceData, referenceData,
    s"$id-given", s"$id-family", s"$id-full", referenceData, random.nextInt, random.nextInt, random.nextInt,
    random.nextString(10), referenceData, random.nextString(5), random.nextString(5), "passport")

  def person(): Person = {
    val id = random.nextString(3)
    Person(id, biographicSet(id), Some(s"$id-createdBy"), Some(s"id-createdDate"), Some(defaultBiographics(id)))
  }

  def refData(id: Int) = {
    ServiceDeliveryRefData(id, s"$id-code", s"$id-short-desc", s"$id-long-desc")
}

  def involvementNotes(id: Int): List[InvolvementNote] = {
    List(
      InvolvementNote(id, refData(random.nextInt()), random.nextString(3)),
      InvolvementNote(id, refData(random.nextInt()), random.nextString(3))
    )
  }

  def correspondences(): Option[List[Correspondence]] = {
    Some(List(
      Correspondence(internalHandle(random.nextString(3)), Some("address-1"), Some("email1"), Some("note1"), Some("created-1"), Some("createdBy-1"), Some("status-1"), Some("lastUpdated-1"), Some("lastUpdatedBy-1")),
      Correspondence(internalHandle(random.nextString(3)), Some("address-2"), Some("email2"), Some("note2"), Some("created-2"), Some("createdBy-2"), Some("status-2"), Some("lastUpdated-2"), Some("lastUpdatedBy-2"))
    ))
  }

  def documentAttachement(id: Int): DocumentAttachment = {
    val docId = random.nextString(3)
    DocumentAttachment(internalHandle(docId), refData(random.nextInt()), s"$id-$docId", docId, refData(random.nextInt()), refData(random.nextInt()), false,
    s"$id-$docId-stageCode", Some("created"), Some("createdBy"), Some("lastUpdated"), Some("lastUpdatedBy"), Some("description"), Some("recordDate"), correspondences)
  }

  def documentAttachments(id: Int) : List[DocumentAttachment] = List(documentAttachement(id), documentAttachement(id))

  def involvement() = {
    val id = random.nextInt()
    Involvement(Some(id), refData(random.nextInt()), Some(refData(random.nextInt())),
      Some(s"$id-createdDate"), Some(s"$id-userId"), Some(person()), Some(internalHandle(s"$id-person")), involvementNotes(id),
      Some(internalHandle(id.toString)), internalHandle(id.toString), documentAttachments(id), Some(s"$id-created"), Some(s"$id-updated"), Some(s"$id-last-updated"), Some(electronicAddresses()), Some(involvementAddresses()))
  }

  def involvements(): List[Involvement] = List(involvement(), involvement())

  def electronicAddress(id: Int): ElectronicAddress = ElectronicAddress(Some(s"$id-type"), Some(s"$id-value"), internalHandle(id.toString), Some(s"$id-created"), Some(s"$id-created-by"), Some(s"$id-effective-start-date"), Some(s"$id-effective-end-date"))

  def electronicAddresses(): List[ElectronicAddress] = List(electronicAddress(random.nextInt()), electronicAddress(random.nextInt()))

  def involvementAddress(id: Int): InvolvementAddress = InvolvementAddress(Some(s"$id-effective-start-date"), Some(s"$id-effective-end-date"), Some(referenceData()), internalHandle(id.toString), Some(internalHandle(s"$id-postal-address-handle")), Some(s"$id-created"), Some(s"$id-created-by"))

  def involvementAddresses(): List[InvolvementAddress] = List(involvementAddress(random.nextInt()), involvementAddress(random.nextInt()))

  def indirectInvolvement(): IndirectInvolvement = {
    val id = random.nextString(5)
    IndirectInvolvement(internalHandle(id), Some(internalHandle(s"$id-organisation")), internalHandle(s"$id-individual"), Some(s"$id-created"), Some(s"$id-created-by"), Some(s"$id-last-updated"), Some(s"$id-last-updated-by"), Some(electronicAddresses()), Some(involvementAddresses()), refData(random.nextInt()), Some(refData(random.nextInt())))
  }

  def indirectInvolvements(): List[IndirectInvolvement] = List(indirectInvolvement(), indirectInvolvement())

  def attribute(): ServiceDeliveryAttribute = {
    val id = random.nextInt
    ServiceDeliveryAttribute(internalHandle(id.toString), id, Some(s"$id-code"), Some(s"$id-date"), Some(s"$id-value"), Some(id),
      Some(referenceDataElement()), Some("created"), Some("createdBy"))
  }

  def attributes(): List[ServiceDeliveryAttribute] = List(attribute(), attribute())

  def process(): ServiceDeliveryProcess = {
    val id = random.nextString(5)
    ServiceDeliveryProcess(internalHandle(id), refData(random.nextInt()), refData(random.nextInt()), Some("createdDate"), Some("createdUserId"),
      Some("lastUpdated"), Some("lastUpdatedUserId"), Some("created"))
  }

  def processes(): List[ServiceDeliveryProcess] = List(process(), process())

  def processInstance(): ProcessInstance = {
    ProcessInstance(random.nextInt(), random.nextInt(), random.nextInt(), random.nextString(3), random.nextInt(), random.nextString(3), Some(random.nextString(6)), Some(random.nextString(6)))
  }

  def processInstances(): List[ProcessInstance] = List(processInstance(), processInstance())

  def serviceDelivery(id: String) = ServiceDelivery(internalHandle(id), Some(s"$id-ext_space"), Some(s"$id-ext_value"),
    refData(random.nextInt()), refData(random.nextInt()), involvements(), indirectInvolvements(), attributes(), processes(),
    documentAttachments(random.nextInt()), processInstances(), Some(refData(random.nextInt())), Some(s"$id-ref-value"), Some(random.nextInt()), Some(s"$id-createdDate"),
    Some(s"$id-lastUpdated"), Some(s"$id-createdUserId"), Some(s"$id-lastUpdatedUserId"), Some(s"$id-parent-service-delivery"),
    Some(s"$id-dueDate"), Some(random.nextInt()), Some(s"$id-created"))

}
