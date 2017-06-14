package uk.gov.ipt.mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov.ipt.mi.model.{Identity, SIdentityReference}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper.emptyHash

@RunWith(classOf[JUnitRunner])
class IdentityReferenceTransformerTest extends FlatSpec with Matchers with Inside {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats // Brings in default date formats etc.

  "Identity References " should "return correct external handle value and space" in {
    val identityJson = scala.io.Source.fromFile("src/test/resources/json/identity-MIBI-451.json").mkString

    val identity = parse(identityJson).extract[Identity]

    val actualReferences = IdentityReferenceTransformer.identityReference("kafkaMessage", identity, System.currentTimeMillis())


    actualReferences should have size 1

    inside(actualReferences.head) {
      case SIdentityReference(_, _, _,
      _, _, _,
      _, _, _,
      reference_ext_handle_value, reference_ext_handle_space, _,
      _, _, _, _, _, identity_hk) =>
        reference_ext_handle_value should not be empty
        reference_ext_handle_value should equal(identity.references.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
        reference_ext_handle_space should not be empty
        reference_ext_handle_space should equal(identity.references.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
        identity_hk should not equal emptyHash
    }
  }
}
