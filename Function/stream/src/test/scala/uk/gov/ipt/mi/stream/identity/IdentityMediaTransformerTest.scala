package uk.gov..mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov..mi.model.{Identity, MediaSet, SIdentityMedia}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._
import uk.gov..mi.stream.ModelHelper._
import uk.gov..mi.stream.identity.IdentityHelper.basicIdentity

@RunWith(classOf[JUnitRunner])
class IdentityMediaTransformerTest extends FlatSpec with Matchers with Inside {

  "Identity Media sets " should "retrun sets when Identity media are missing" in {
    val mediaSet = MediaSet(internalHandle("ih-mediaset"), Some(externalHandle("ex-mediaseet")), Some("mediaSetCreated"), None, None)

    val identity = basicIdentity()

    val actualMediaSets = IdentityMediaTransformer.identityMedias(Some(Set(mediaSet)), "kafkaMessageId", identity, System.currentTimeMillis())

    actualMediaSets should have size 1

    inside(actualMediaSets.head) {
      case SIdentityMedia(_, _, _,
      _, identity_handle_id, media_superset_agg_hash,
      media_set_handle_id, media_set_rec_hash_value, media_set_handle_visibility,
      media_set_ext_handle_value, media_set_ext_handle_space, media_set_created_by,
      media_set_created_datetime, media_set_agg_hash, lnk_idntty_media_set_hk,
      media_handle_id, media_rec_seqno, media_rec_hash_value,
      media_handle_visibility, media_type_cd, media_file_handle_id,
      media_created_by, media_created_datetime, identity_hk) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        media_superset_agg_hash should not equal emptyHash
        media_set_handle_id should equal(mediaSet.internalHandle.interface_identifier)
        media_set_rec_hash_value should not equal emptyHash
        media_set_handle_visibility should equal(mediaSet.internalHandle.visibility_marker)
        media_set_ext_handle_value should equal(mediaSet.externalHandle.flatMap(_.handle_value))
        media_set_ext_handle_space should equal(mediaSet.externalHandle.flatMap(_.handle_space))
        media_set_created_by should equal(mediaSet.createdBy)
        media_set_created_datetime should equal(mediaSet.created)
        media_set_agg_hash should equal(emptyHash)
        lnk_idntty_media_set_hk should not equal emptyHash
        media_handle_id shouldBe None
        media_rec_seqno shouldBe None
        media_rec_hash_value shouldBe None
        media_handle_visibility shouldBe None
        media_type_cd shouldBe None
        media_file_handle_id shouldBe None
        media_created_by shouldBe None
        media_created_datetime shouldBe None
        identity_hk should not equal emptyHash
    }
  }

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats // Brings in default date formats etc.

  "Identity mediaset" should "return correct external handle value, space and media created tine" in {
    val identityJson = scala.io.Source.fromFile("src/test/resources/json/identity-MIBI-451-media-set.json").mkString

    val identity = parse(identityJson).extract[Identity]

    val actualMediaSets = IdentityMediaTransformer.identityMedias(identity.identity_media_sets, "kafkaMessageId", identity, System.currentTimeMillis())

    actualMediaSets should have size 1

    inside(actualMediaSets.head) {
      case SIdentityMedia(_, _, _,
      _, _, _,
      _, _, _,
      media_set_ext_handle_value, media_set_ext_handle_space, _,
      _, _, _,
      _, _, _,
      _, _, _,
      _, media_created_datetime, identity_hk) =>
        media_set_ext_handle_value should not be empty
        media_set_ext_handle_value should equal(identity.identity_media_sets.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
        media_set_ext_handle_space should not be empty
        media_set_ext_handle_space should equal(identity.identity_media_sets.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
        media_created_datetime should not be empty
        media_created_datetime should equal(identity.identity_media_sets.flatMap(_.toList.sorted.head.created))
        identity_hk should not equal emptyHash
    }

  }

}
