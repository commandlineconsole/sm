package uk.gov.ipt.mi.model.servicedelivery

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import uk.gov.ipt.mi.model.InternalHandle
import scala.Option.empty

case class ReferenceDataElement(code: String, id: Long)

case class ServiceDeliveryAttribute(internalHandle: InternalHandle, serviceDeliveryAttributeTypeId: BigInt, serviceDeliveryAttributeTypeCode: Option[String],
                                    attributeValueDate: Option[String], attributeValueString: Option[String], attributeValueNumber: Option[Int],
                                    attributeValueRefDataElement: Option[ReferenceDataElement], created: Option[String], createdBy: Option[String]) extends Ordered[ServiceDeliveryAttribute] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: ServiceDeliveryAttribute): Int = (this.internalHandle.interface_identifier, this.serviceDeliveryAttributeTypeId, this.serviceDeliveryAttributeTypeCode) compare(that.internalHandle.interface_identifier, that.serviceDeliveryAttributeTypeId, that.serviceDeliveryAttributeTypeCode)
}

case class Biographic(internalHandle: Option[InternalHandle], biographicType: String, biographicValue: String) extends Ordered[Biographic] {
  import scala.math.Ordered.orderingToOrdered

  def compare(that: Biographic): Int = (this.internalHandle.map(_.interface_identifier), this.biographicType, this.biographicValue) compare(that.internalHandle.map(_.interface_identifier), that.biographicType, that.biographicValue)
}

case class ReferenceData(code: String, shortDescription: String, longDescription: String)

case class DefaultBiographics(identityHandle: InternalHandle, title: ReferenceData, language: ReferenceData,
                              givenName: String, familyName: String, fullName: String,
                              gender: ReferenceData, dayOfBirth: Int, monthOfBirth: Int,
                              yearOfBirth: Int, photoUrl: String, nationality: ReferenceData,
                              placeOfBirth: String, dateOfBirth: String, passport: String)

case class Person(personId: String, recentBiographics: Set[Biographic], createdUserId: Option[String],
                  createdDate: Option[String], defaultBiographics: Option[DefaultBiographics])

case class ServiceDeliveryRefData(refDataValueId: BigInt, refDataValueCode: String, refDataValueShortDesc: String, refDataValueLongDesc: String)

case class InvolvementNote(id: BigInt, `type`: ServiceDeliveryRefData, noteText: String) extends Ordered[InvolvementNote] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: InvolvementNote): Int = (this.id, this.`type`.toString, this.noteText) compare(that.id, that.`type`.toString, that.noteText)
}

case class Correspondence(internalHandle: InternalHandle, postalAddressId: Option[String], electronicAddressId: Option[String],
                          deliveryNote: Option[String], created: Option[String], createdBy: Option[String],
                          status: Option[String], lastUpdated: Option[String], lastUpdatedBy: Option[String]) extends Ordered[Correspondence] {

  def compare(that: Correspondence): Int = (this.internalHandle.interface_identifier) compare(that.internalHandle.interface_identifier)
}

case class DocumentAttachment(internalHandle: InternalHandle, attachmentType: ServiceDeliveryRefData, externalReference: String,
                              documentStoreId: String, mimeType: ServiceDeliveryRefData, provider: ServiceDeliveryRefData,
                              verified: Boolean, stageCode: String, created: Option[String],
                              createdBy: Option[String], lastUpdated: Option[String], lastUpdatedBy: Option[String],
                              description: Option[String], recordDate:Option[String],correspondences:Option[List[Correspondence]]) extends Ordered[DocumentAttachment]{

  import scala.math.Ordered.orderingToOrdered

  def compare(that: DocumentAttachment): Int = (this.internalHandle.interface_identifier, this.attachmentType.refDataValueCode) compare (that.internalHandle.interface_identifier, that.attachmentType.refDataValueCode)
}

case class Involvement(id: Option[BigInt], involvementRoleType: ServiceDeliveryRefData, involvementRoleSubType: Option[ServiceDeliveryRefData],
                       createdDate: Option[String], createdUserId: Option[String], person: Option[Person],
                       personHandle: Option[InternalHandle], notes: List[InvolvementNote], organisationHandle: Option[InternalHandle],
                       internalHandle: InternalHandle, documentAttachments: List[DocumentAttachment], created: Option[String],
                       lastUpdated: Option[String], lastUpdatedUserId: Option[String], electronicAddresses: Option[List[ElectronicAddress]],
                       involvementAddresses: Option[List[InvolvementAddress]]) extends Ordered[Involvement] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Involvement): Int = (this.internalHandle.interface_identifier, this.involvementRoleType.refDataValueId, this.involvementRoleType.refDataValueId) compare(that.internalHandle.interface_identifier, that.involvementRoleType.refDataValueId, that.involvementRoleType.refDataValueId)

}


case class IndirectInvolvement(internalHandle: InternalHandle, organisationHandle: Option[InternalHandle], individualHandle: InternalHandle,
                               created: Option[String], createdBy: Option[String], lastUpdated: Option[String],
                               lastUpdatedUserId: Option[String], electronicAddresses: Option[List[ElectronicAddress]], involvementAddresses: Option[List[InvolvementAddress]],
                               involvementRoleType: ServiceDeliveryRefData, involvementRoleSubType: Option[ServiceDeliveryRefData]) extends Ordered[IndirectInvolvement] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: IndirectInvolvement): Int = (this.internalHandle.interface_identifier, this.organisationHandle.map(_.interface_identifier), this.individualHandle.interface_identifier) compare (that.internalHandle.interface_identifier, that.organisationHandle.map(_.interface_identifier), that.individualHandle.interface_identifier)
}

case class ServiceDeliveryProcess(internalHandle: InternalHandle, serviceDeliveryStage: ServiceDeliveryRefData, processStatus: ServiceDeliveryRefData,
                                  createdDate: Option[String], createdUserId: Option[String], lastUpdated: Option[String],
                                  lastUpdatedUserId: Option[String], created: Option[String]) extends Ordered[ServiceDeliveryProcess] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: ServiceDeliveryProcess): Int = (this.internalHandle.interface_identifier, this.serviceDeliveryStage.refDataValueCode, this.processStatus.refDataValueCode) compare(that.internalHandle.interface_identifier, that.serviceDeliveryStage.refDataValueCode, that.processStatus.refDataValueCode)
}

case class ProcessInstance(id: BigInt, processId: BigInt, processStatusId: BigInt,
                           processStatusCode: String, stageId: BigInt, stageCode: String,
                           startDateTime:Option[String], endDateTime: Option[String]) extends Ordered[ProcessInstance] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: ProcessInstance): Int = (this.id, this.processId, this.processStatusCode, this.stageCode) compare(that.id, that.processId, that.processStatusCode, that.stageCode)
}

case class ServiceDelivery(internalHandle: InternalHandle, externalHandleSpace: Option[String], externalHandleValue: Option[String],
                           serviceDeliveryType: ServiceDeliveryRefData, serviceDeliveryStatus: ServiceDeliveryRefData, involvements: List[Involvement],
                           indirectInvolvements: List[IndirectInvolvement], attributes: List[ServiceDeliveryAttribute], serviceDeliveryProcesses: List[ServiceDeliveryProcess],
                           documentAttachments: List[DocumentAttachment], processInstances: List[ProcessInstance], primaryRefType: Option[ServiceDeliveryRefData],
                           primaryRefValue: Option[String], id: Option[BigInt], createdDate: Option[String],
                           lastUpdated: Option[String], createdUserId: Option[String], lastUpdatedUserId: Option[String],
                           parentServiceDelivery: Option[String], dueDate: Option[String], priority: Option[Int],
                           created: Option[String])

case class ElectronicAddress(`type`: Option[String], value: Option[String], internalHandle: InternalHandle,
                        created: Option[String], createdBy: Option[String], effectiveStartDate: Option[String],
                        effectiveEndDate: Option[String]) extends Ordered[ElectronicAddress] {

  import scala.math.Ordered.orderingToOrdered

  override def compare(that: ElectronicAddress): Int = (this.internalHandle.interface_identifier, this.`type`, this.value) compare (that.internalHandle.interface_identifier, that.`type`, that.value)
}

case class InvolvementAddress(effectiveStartDate: Option[String], effectiveEndDate: Option[String], addressUsageType: Option[ReferenceData],
                              internalHandle: InternalHandle, postalAddressHandle: Option[InternalHandle], created: Option[String],
                              createdBy: Option[String]) extends Ordered[InvolvementAddress] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: InvolvementAddress): Int = (this.internalHandle.interface_identifier, this.addressUsageType.map(_.code)) compare (that.internalHandle.interface_identifier, that.addressUsageType.map(_.code))

}
