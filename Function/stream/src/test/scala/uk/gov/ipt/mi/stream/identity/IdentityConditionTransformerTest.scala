package uk.gov..mi.stream.identity

import org.junit.runner.RunWith
import org.scalatest.{FlatSpec, Inside, Matchers}
import org.scalatest.junit.JUnitRunner
import uk.gov..mi.model.{Identity, SIdentityCondition}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper.emptyHash
import scala.io.Source


@RunWith(classOf[JUnitRunner])
class IdentityConditionTransformerTest extends FlatSpec with Matchers with Inside {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats // Brings in default date formats etc.


  "Identity conditions " should "return correct external handle value and space " in {
    val fileStream = getClass.getResourceAsStream("/json/identity-MIBI-451.json")
    val identityJson = Source.fromInputStream(fileStream).mkString

    val identity = parse(identityJson).extract[Identity]

    val actualConditions = IdentityConditionTransformer.identityCondition("messageId", identity, System.currentTimeMillis())

    actualConditions should have size 2

    inside(actualConditions.head){
      case SIdentityCondition(_, _, _,
      _, _, _,
      _, _, _,
      condition_ext_handle_value, condition_ext_handle_space, _,
      _, condition_type_cd, _,
      _, _, _,
      _, _, _,
      identity_hk) =>
        condition_ext_handle_value should not be empty
        identity_hk should not equal emptyHash
        condition_ext_handle_value should equal(identity.conditions.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
        condition_ext_handle_space should not be empty
        condition_ext_handle_space should equal(identity.conditions.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
        condition_type_cd should equal(identity.conditions.flatMap(_.toList.sorted.head.conditionType).flatMap(_.code))
    }
  }

}
