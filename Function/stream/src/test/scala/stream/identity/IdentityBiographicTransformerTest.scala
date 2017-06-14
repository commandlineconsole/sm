package uk.gov..mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov..mi.model._
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper.emptyHash
import uk.gov..mi.stream.ModelHelper._
import uk.gov..mi.stream.identity.IdentityHelper._


@RunWith(classOf[JUnitRunner])
class IdentityBiographicTransformerTest extends FlatSpec with Matchers with Inside {

  "Identity Biographic sets " should "return biographics set when biographics are missing" in {
    val biographicSet = BiographicSet(internalHandle("internal"), Some(externalHandle("external")),
      Some(ReferenceType(Some("code"), None, None)), Some("created"), Some("createdBy"), Map())
    val identity = basicIdentity()

    val actualBiographicSets = IdentityBiographicTransformer.identityBiographics(Some(Set(biographicSet)), "kafkaMessageId", identity, System.currentTimeMillis())

    actualBiographicSets should have size 1

    inside(actualBiographicSets.head) {
      case SIdentityBiographics(_, _, _,
      _, identity_handle_id, biog_superset_agg_hash,
      biog_set_handle_id, biog_set_rec_hash_value: String, biog_set_handle_visibility: Option[String],
      biog_set_ext_handle_value: Option[String], biog_set_ext_handle_space: Option[String], biog_set_purpose_cd,
      biog_set_created_by, biog_set_created_datetime, biog_set_agg_hash,
      lnk_idntty_biog_set_hk, biographic_key, biographic_handle_id, biographic_rec_seqno,
      biographic_rec_hash_value, biographic_handle_visibility, biographic_type_cd,
      biographic_value, biographic_value_type_cd, biographic_created_by,
      biographic_created_datetime, biographic_reference_data_set, hk_identity) =>
        identity_handle_id should equal(identity.internal_handle.interface_identifier)
        biog_superset_agg_hash should not equal emptyHash
        biog_set_handle_id should equal(biographicSet.internal_handle.interface_identifier)
        biog_set_rec_hash_value should not equal emptyHash
        biog_set_handle_visibility should equal(biographicSet.internal_handle.visibility_marker)
        biog_set_ext_handle_value should equal(biographicSet.external_handle.flatMap(_.handle_value))
        biog_set_ext_handle_space should equal(biographicSet.external_handle.flatMap(_.handle_space))
        biog_set_purpose_cd should equal(biographicSet.biographic_set_purpose.flatMap(_.code))
        biog_set_created_by should equal(biographicSet.created_by)
        biog_set_created_datetime should equal(biographicSet.created)
        biog_set_agg_hash should equal(emptyHash)
        lnk_idntty_biog_set_hk should not equal emptyHash
        biographic_key shouldBe None
        biographic_handle_id shouldBe None
        biographic_rec_seqno shouldBe None
        biographic_rec_hash_value shouldBe None
        biographic_handle_visibility shouldBe None
        biographic_type_cd shouldBe None
        biographic_value shouldBe None
        biographic_value_type_cd shouldBe None
        biographic_created_by shouldBe None
        biographic_created_datetime shouldBe None
        biographic_reference_data_set shouldBe None
        hk_identity should not equal emptyHash
    }
  }

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats // Brings in default date formats etc.


  "Identity Biographic set " should "retrun correct external handle value and space " in {
    val identityJson = scala.io.Source.fromFile("src/test/resources/json/identity-MIBI-451.json").mkString

    val identity = parse(identityJson).extract[Identity]

    val actualBiographicSets = IdentityBiographicTransformer.identityBiographics(identity.biographic_sets, "kafkaMessageId", identity, System.currentTimeMillis())

    actualBiographicSets should have size 15

    inside(actualBiographicSets.head) {
      case SIdentityBiographics(_, _, _,
      _, _, _,
      _, _, _,
      biog_set_ext_handle_value: Option[String], biog_set_ext_handle_space: Option[String], biog_set_purpose_cd,
      _, _, _,
      _, _, _, _,
      _, _, _,
      _, _, _,
      _, _, identity_hk) =>
        biog_set_ext_handle_value should not be empty
        biog_set_ext_handle_value should equal(identity.biographic_sets.flatMap(_.toList.sorted.head.external_handle.flatMap(_.handle_value)))
        biog_set_ext_handle_space should not be empty
        biog_set_ext_handle_space should equal(identity.biographic_sets.flatMap(_.toList.sorted.head.external_handle.flatMap(_.handle_space)))
        identity_hk should not be emptyHash
    }
  }

  "Identity Biographic set " should "not produce duplicates " in {
    val identities: List[Identity] = scala.io.Source.fromFile("src/test/resources/json/identity-MIBI-451-duplicates.json").getLines.map(identityJson => parse(identityJson).extract[Identity]).toList

    val actualBiographics: List[SIdentityBiographics] = identities.flatMap(identity => IdentityBiographicTransformer.identityBiographics(identity.biographic_sets, "kafkaMessageId", identity, System.currentTimeMillis()))


    actualBiographics should have size identities.foldLeft(0)((size, identity) => size + identity.biographic_sets.map(biogSet => biogSet.foldLeft(0)((setSize, set) => setSize + set.biographics.size)).getOrElse(0))

    actualBiographics.filter(_.biographic_type_cd == Some("GENDER")) should have size identities.foldLeft(0)((size, identity) => size + identity.biographic_sets.map(biogSet => biogSet.foldLeft(0)((setSize, set) => setSize + set.biographics.filter(_._1.equals("GENDER")).size)).getOrElse(0))
  }
}
