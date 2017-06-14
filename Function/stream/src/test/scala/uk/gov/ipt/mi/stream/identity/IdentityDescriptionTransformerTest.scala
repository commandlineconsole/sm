package uk.gov..mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov..mi.model.{Descrion, Descror, SIdentityDescrors}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._
import uk.gov..mi.stream.ModelHelper._
import uk.gov..mi.stream.identity.IdentityHelper.basicIdentity


@RunWith(classOf[JUnitRunner])
class IdentityDescrionTransformerTest extends FlatSpec with Matchers with Inside {

  "Identity descrions" should "return identity sets when descrors are missing" in {
    val descrionWithEmptyDescror = Descrion(internalHandle("id"), Some(externalHandle("ext-")), Some("created"), Some("createdBy"), None)
    val identity = basicIdentity()

    val actualSIdentityDescrors = IdentityDescrionTransformer.identityDescrors(Some(Set(descrionWithEmptyDescror)), "kafkaId", identity, System.currentTimeMillis())

    actualSIdentityDescrors should have size 1

    inside(actualSIdentityDescrors.head) {
      case SIdentityDescrors(_, _, _,
      _, identity_handle_id, _,
      descr_set_handle_id, _, descr_set_handle_visibility,
      descr_set_ext_handle_value, descr_set_ext_handle_space, descr_set_created_by,
      descr_set_created_datetime, descr_set_agg_hash, _,
      descr_handle_id, descr_rec_seqno, descr_rec_hash_value,
      descr_handle_visibility, descr_type_cd, descr_value,
      descr_created_by, descr_created_datetime, identity_hk) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        descr_set_handle_id should equal(descrionWithEmptyDescror.internalHandle.interface_identifier)
        descr_set_handle_visibility should equal(descrionWithEmptyDescror.internalHandle.visibility_marker)
        descr_set_ext_handle_value should equal(descrionWithEmptyDescror.externalHandle.flatMap(_.handle_value))
        descr_set_ext_handle_space should equal(descrionWithEmptyDescror.externalHandle.flatMap(_.handle_space))
        descr_set_created_by should equal(descrionWithEmptyDescror.createdBy)
        descr_set_created_datetime should equal(descrionWithEmptyDescror.created)
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

  "Identity descrions" should "return an IdentityDescrion per descror" in {
    val identity = IdentityHelper.createIdentity("JohnDoe-ih", "JohnDoe-ext")

    val actualSIdDescrors = IdentityDescrionTransformer
      .identityDescrors(identity.descrions, "kafkaMId", identity, System.currentTimeMillis()).sortBy(_.descr_rec_seqno)

    actualSIdDescrors should have size 2
    actualSIdDescrors.head.descr_rec_seqno shouldBe Some(0)
    actualSIdDescrors.last.descr_rec_seqno shouldBe Some(1)

    val headDescrion: Descrion = identity.descrions.map(_.toList.sorted.head).get
    val headDescror: Descror = headDescrion.descrors.map(_.toList.sorted.head).get

    inside(actualSIdDescrors.head) {
      case SIdentityDescrors(_, _, _,
      _, identity_handle_id, _,
      descr_set_handle_id, _, descr_set_handle_visibility,
      descr_set_ext_handle_value, descr_set_ext_handle_space, descr_set_created_by,
      descr_set_created_datetime, descr_set_agg_hash, _,
      descr_handle_id, descr_rec_seqno, descr_rec_hash_value,
      descr_handle_visibility, descr_type_cd, descr_value,
      descr_created_by, descr_created_datetime, identity_hk) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        descr_set_handle_id should equal(headDescrion.internalHandle.interface_identifier)
        descr_set_handle_visibility should equal(headDescrion.internalHandle.visibility_marker)
        descr_set_ext_handle_value should equal(headDescrion.externalHandle.flatMap(_.handle_value))
        descr_set_ext_handle_space should equal(headDescrion.externalHandle.flatMap(_.handle_space))
        descr_set_created_by should equal(headDescrion.createdBy)
        descr_set_created_datetime should equal(headDescrion.created)
        descr_set_agg_hash should not equal emptyHash
        descr_handle_id shouldBe Some(headDescror.internalHandle.interface_identifier)
        descr_rec_seqno shouldBe Some(0)
        descr_rec_hash_value should not equal emptyHash
        descr_handle_visibility shouldBe headDescror.internalHandle.visibility_marker
        descr_type_cd shouldBe headDescror.descrorType
        descr_value shouldBe headDescror.descrorValue
        descr_created_by shouldBe headDescror.createdBy
        descr_created_datetime shouldBe headDescror.created
        identity_hk should not equal emptyHash
    }
  }
}
