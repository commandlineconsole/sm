package uk.gov.ipt.mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov.ipt.mi.model.{Description, Descriptor, SIdentityDescriptors}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper._
import uk.gov.ipt.mi.stream.ModelHelper._
import uk.gov.ipt.mi.stream.identity.IdentityHelper.basicIdentity


@RunWith(classOf[JUnitRunner])
class IdentityDescriptionTransformerTest extends FlatSpec with Matchers with Inside {

  "Identity descriptions" should "return identity sets when descriptors are missing" in {
    val descriptionWithEmptyDescriptor = Description(internalHandle("id"), Some(externalHandle("ext-")), Some("created"), Some("createdBy"), None)
    val identity = basicIdentity()

    val actualSIdentityDescriptors = IdentityDescriptionTransformer.identityDescriptors(Some(Set(descriptionWithEmptyDescriptor)), "kafkaId", identity, System.currentTimeMillis())

    actualSIdentityDescriptors should have size 1

    inside(actualSIdentityDescriptors.head) {
      case SIdentityDescriptors(_, _, _,
      _, identity_handle_id, _,
      descr_set_handle_id, _, descr_set_handle_visibility,
      descr_set_ext_handle_value, descr_set_ext_handle_space, descr_set_created_by,
      descr_set_created_datetime, descr_set_agg_hash, _,
      descr_handle_id, descr_rec_seqno, descr_rec_hash_value,
      descr_handle_visibility, descr_type_cd, descr_value,
      descr_created_by, descr_created_datetime, identity_hk) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        descr_set_handle_id should equal(descriptionWithEmptyDescriptor.internalHandle.interface_identifier)
        descr_set_handle_visibility should equal(descriptionWithEmptyDescriptor.internalHandle.visibility_marker)
        descr_set_ext_handle_value should equal(descriptionWithEmptyDescriptor.externalHandle.flatMap(_.handle_value))
        descr_set_ext_handle_space should equal(descriptionWithEmptyDescriptor.externalHandle.flatMap(_.handle_space))
        descr_set_created_by should equal(descriptionWithEmptyDescriptor.createdBy)
        descr_set_created_datetime should equal(descriptionWithEmptyDescriptor.created)
        descr_set_agg_hash should equal(emptyHash)
        descr_handle_id shouldBe None
        descr_rec_seqno shouldBe None
        descr_rec_hash_value shouldBe None
        descr_handle_visibility shouldBe None
        descr_type_cd shouldBe None
        descr_value shouldBe None
        descr_created_by shouldBe None
        descr_created_datetime shouldBe None
        identity_hk should not equal emptyHash
    }
  }

  "Identity descriptions" should "return an IdentityDescription per descriptor" in {
    val identity = IdentityHelper.createIdentity("JohnDoe-ih", "JohnDoe-ext")

    val actualSIdDescriptors = IdentityDescriptionTransformer
      .identityDescriptors(identity.descriptions, "kafkaMId", identity, System.currentTimeMillis()).sortBy(_.descr_rec_seqno)

    actualSIdDescriptors should have size 2
    actualSIdDescriptors.head.descr_rec_seqno shouldBe Some(0)
    actualSIdDescriptors.last.descr_rec_seqno shouldBe Some(1)

    val headDescription: Description = identity.descriptions.map(_.toList.sorted.head).get
    val headDescriptor: Descriptor = headDescription.descriptors.map(_.toList.sorted.head).get

    inside(actualSIdDescriptors.head) {
      case SIdentityDescriptors(_, _, _,
      _, identity_handle_id, _,
      descr_set_handle_id, _, descr_set_handle_visibility,
      descr_set_ext_handle_value, descr_set_ext_handle_space, descr_set_created_by,
      descr_set_created_datetime, descr_set_agg_hash, _,
      descr_handle_id, descr_rec_seqno, descr_rec_hash_value,
      descr_handle_visibility, descr_type_cd, descr_value,
      descr_created_by, descr_created_datetime, identity_hk) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        descr_set_handle_id should equal(headDescription.internalHandle.interface_identifier)
        descr_set_handle_visibility should equal(headDescription.internalHandle.visibility_marker)
        descr_set_ext_handle_value should equal(headDescription.externalHandle.flatMap(_.handle_value))
        descr_set_ext_handle_space should equal(headDescription.externalHandle.flatMap(_.handle_space))
        descr_set_created_by should equal(headDescription.createdBy)
        descr_set_created_datetime should equal(headDescription.created)
        descr_set_agg_hash should not equal emptyHash
        descr_handle_id shouldBe Some(headDescriptor.internalHandle.interface_identifier)
        descr_rec_seqno shouldBe Some(0)
        descr_rec_hash_value should not equal emptyHash
        descr_handle_visibility shouldBe headDescriptor.internalHandle.visibility_marker
        descr_type_cd shouldBe headDescriptor.descriptorType
        descr_value shouldBe headDescriptor.descriptorValue
        descr_created_by shouldBe headDescriptor.createdBy
        descr_created_datetime shouldBe headDescriptor.created
        identity_hk should not equal emptyHash
    }
  }
}
