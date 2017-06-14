package uk.gov.ipt.mi.model

case class InternalHandle(interface_identifier: String, unique_identifier: String, visibility_marker: Option[String])

case class ExternalHandle(unique_identifier: String, handle_value: Option[String], handle_space: Option[String])

case class Person(internal_handle: InternalHandle, person_space: Option[String], created: Option[String],
                  created_by: Option[String], identity_handles: Set[InternalHandle])

case class ReferenceType(code: Option[String], shortDescription: Option[String], longDescription: Option[String]) extends Ordered[ReferenceType] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: ReferenceType): Int = (this.code, this.shortDescription, this.longDescription) compare(that.code, that.shortDescription, that.longDescription)
}

case class Biographic(internal_handle: InternalHandle, `type`: Option[String], value: Option[String], value_type: Option[String],
                      created: Option[String], created_by: Option[String], reference_data_set: Option[String]) extends Ordered[Biographic] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Biographic): Int = (this.internal_handle.interface_identifier, this.`type`) compare(that.internal_handle.interface_identifier, that.`type`)
}

case class BiographicSet(internal_handle: InternalHandle, external_handle: Option[ExternalHandle], biographic_set_purpose: Option[ReferenceType],
                         created: Option[String], created_by: Option[String], biographics: Map[String, Biographic]) extends Ordered[BiographicSet] {

  def compare(that: BiographicSet): Int = (this.internal_handle.interface_identifier) compare (that.internal_handle.interface_identifier)

}

case class Biometric(internalHandle: InternalHandle, externalHandle: Option[ExternalHandle], biometricReferenceType: Option[String],
                     biometricNature: Option[String], biometricReferenceValue: Option[String], created: Option[String], created_by: Option[String]) extends Ordered[Biometric] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Biometric): Int = (this.internalHandle.interface_identifier, this.biometricNature) compare(that.internalHandle.interface_identifier, that.biometricNature)
}

case class IdentityCondition(internalHandle: InternalHandle, externalHandle: Option[ExternalHandle], conditionType: Option[ReferenceType],
                             conditionDate: Option[String], conditionUpdateDate: Option[String], conditionNote: Option[String],
                             conditionEndDate: Option[String], created: Option[String], createdBy: Option[String], originatingUser: Option[String]) extends Ordered[IdentityCondition] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: IdentityCondition): Int = (this.internalHandle.interface_identifier, this.conditionType) compare(that.internalHandle.interface_identifier, that.conditionType)

}

case class Descriptor(internalHandle: InternalHandle, descriptorType: Option[String], descriptorValue: Option[String], created: Option[String],
                      createdBy: Option[String]) extends Ordered[Descriptor] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Descriptor): Int = (this.internalHandle.interface_identifier, this.descriptorType, this.descriptorValue) compare(that.internalHandle.interface_identifier, that.descriptorType, that.descriptorValue)
}

case class Description(internalHandle: InternalHandle, externalHandle: Option[ExternalHandle], created: Option[String], createdBy: Option[String],
                       descriptors: Option[Set[Descriptor]]) extends Ordered[Description] {

  def compare(that: Description): Int = (this.internalHandle.interface_identifier) compare (that.internalHandle.interface_identifier)
}

case class Reference(internalHandle: InternalHandle, externalHandle: Option[ExternalHandle], referenceType: Option[ReferenceType],
                     referenceValue: Option[String], created: Option[String], createdBy: Option[String]) extends Ordered[Reference] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: Reference): Int = (this.internalHandle.interface_identifier, this.referenceType, this.referenceValue) compare(that.internalHandle.interface_identifier, that.referenceType, that.referenceValue)
}

case class IdentityMedia(internalHandle: InternalHandle, identityMediaType: Option[String], fileHandle: Option[InternalHandle],
                         created: Option[String], createdBy: Option[String]) extends Ordered[IdentityMedia] {

  import scala.math.Ordered.orderingToOrdered

  def compare(that: IdentityMedia): Int = (this.internalHandle.interface_identifier, this.identityMediaType) compare(that.internalHandle.interface_identifier, that.identityMediaType)
}

case class MediaSet(internalHandle: InternalHandle, externalHandle: Option[ExternalHandle], created: Option[String], createdBy: Option[String],
                    identityMedias: Option[Set[IdentityMedia]]) extends Ordered[MediaSet] {

  def compare(that: MediaSet): Int = (this.internalHandle.interface_identifier) compare (that.internalHandle.interface_identifier)
}

case class AuditHistory(created_by: Option[String], created: Option[String], lastupdated_by: Option[String], lastupdated: Option[String])

case class Identity(internal_handle: InternalHandle, external_handle: Option[ExternalHandle], created: Option[String], created_by: Option[String],
                    biographic_sets: Option[Set[BiographicSet]], biometrics: Option[Set[Biometric]], descriptions: Option[Set[Description]],
                    references: Option[Set[Reference]], conditions: Option[Set[IdentityCondition]], identity_media_sets: Option[Set[MediaSet]],
                    containing_person_handle: Option[InternalHandle], source: Option[String], auditHistory: Option[AuditHistory])
