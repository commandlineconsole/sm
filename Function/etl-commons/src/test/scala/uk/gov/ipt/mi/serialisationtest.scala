package uk.gov..mi

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Inside, Matchers}
import uk.gov..mi.model._
import uk.gov..mi.model.servicedelivery.ServiceDelivery


@RunWith(classOf[JUnitRunner])
class SerialisationSuite extends FunSuite with Matchers with Inside {

  val personJson =
    s"""{
       |  "internal_handle": {
       |    "unique_identifier": "09365212c31e11e36e4dd510165ffc5725d745093cd689a6f597489b373d1d6c",
       |    "visibility_marker": "DEFAULT",
       |    "interface_identifier": "CID_PEOPLE_102154"
       |  },
       |  "person_space": "",
       |  "created": "2016-09-27T14:09:38.473656",
       |  "created_by": null,
       |  "identity_handles": [
       |    {
       |      "unique_identifier": "09365212c31e11e36e4dd510165ffc5725d745093cd689a6f597489b373d1d6c",
       |      "visibility_marker": "DEFAULT",
       |      "interface_identifier": "CID_PEOPLE_102154"
       |    },
       |    {
       |      "unique_identifier": "09365212c31e11e36e4dd510165ffc5725d745093cd689a6f597489b373d1d6d",
       |      "visibility_marker": "DEFAULT",
       |      "interface_identifier": "CID_PEOPLE_102155"
       |    }
       |  ]
       |}""".stripMargin

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val formats = DefaultFormats // Brings in default date formats etc.

  test("Person serialisation should accept snake cases") {
    val person = parse(personJson).extract[Person]
    person.internal_handle.visibility_marker should be(Some("DEFAULT"))
    person.identity_handles.size should be(2)
    person.person_space should be(Some(""))
  }

  test("Identity serialisation should accept snake cases") {
    val identityJson = scala.io.Source.fromFile("src/test/resources/json/IdentityV2.json").mkString

    val identity = parse(identityJson).extract[Identity]

    identity.source should be(Some("CRS"))

    inside(identity.internal_handle) {
      case InternalHandle(interfaceIdentifier, _, _) => interfaceIdentifier should be("CRS_LOCAL_ALERT_50_103826")
    }

    inside(identity.references.get.toSeq) {
      case Seq(Reference(_, _, Some(ReferenceType(Some(code), _, _)), Some(referenceValue), _, createdBy)) =>
        code should be("PASSPORT")
        referenceValue should be("1233241234234")
        createdBy should be(None)
    }

    inside(identity.containing_person_handle.get) {
      case InternalHandle(interfaceIdentifier, _, Some(visibility)) =>
        interfaceIdentifier should be("CRS_LOCAL_ALERT_50_103826")
        visibility should be("DEFAULT")
    }

    inside(identity.biographic_sets.get.toSeq) {
      case Seq(BiographicSet(internalHandle, _, Some(biographicSetPurpose), _, _, biographics)) =>
        biographicSetPurpose.code should be(Some("CB"))
        internalHandle.interface_identifier should be("CRS_LOCAL_ALERT_50_103826")
        biographics.get("FULL_NAME") should not be None
        biographics.get("FULL_NAME").get.value should be(Some("Barbara Chambers"))
    }
  }

  test("Service delivery serialisation should accept all cases") {
    val serviceDeliveryJson = scala.io.Source.fromFile("src/test/resources/json/ServiceDelivery.json").mkString

    val serviceDelivery = parse(serviceDeliveryJson).extract[ServiceDelivery]

    serviceDelivery.externalHandleSpace should equal(Some("CRS_APPLICATION"))
    serviceDelivery.externalHandleValue should equal(Some("154_277852"))
    serviceDelivery.internalHandle.interface_identifier should equal("CRS_APPLICATION_154_277852")
    serviceDelivery.internalHandle.visibility_marker should equal(Some("DEFAULT"))
    serviceDelivery.serviceDeliveryType.refDataValueCode should equal("LTE")
    serviceDelivery.serviceDeliveryType.refDataValueId should equal(10305)
    serviceDelivery.serviceDeliveryStatus.refDataValueCode should equal("EXTERNAL")
    serviceDelivery.serviceDeliveryStatus.refDataValueId should equal(10605)
    serviceDelivery.lastUpdated should equal(Some("2016-12-19T15:44:59Z"))
    serviceDelivery.attributes.sorted.head.internalHandle.interface_identifier should equal("BCID_VIS_277853")
    serviceDelivery.attributes.sorted.head.serviceDeliveryAttributeTypeId should equal(10709)
    serviceDelivery.attributes.sorted.head.serviceDeliveryAttributeTypeCode should equal(Some("BCID"))
    serviceDelivery.involvements.sorted.head.internalHandle.interface_identifier should equal("CRS_NAMED_PERSON_APPLICATION_APPLICANT_277852_154_277852")
    serviceDelivery.involvements.sorted.head.involvementRoleType.refDataValueId should equal(11500)
    serviceDelivery.involvements.sorted.head.involvementRoleType.refDataValueCode should equal("APPLICANT")
    serviceDelivery.involvements.sorted.head.personHandle.map(_.interface_identifier) should equal(Some("CRS_NAMED_PERSON_154_277852"))
  }

  test("Service delivery serialisation should accept RefData") {
    val serviceDeliveryJson = scala.io.Source.fromFile("src/test/resources/json/ServiceDelivery-MIBI-539.json").mkString

    val serviceDelivery = parse(serviceDeliveryJson).extract[ServiceDelivery]

    serviceDelivery.attributes should have size 6
    serviceDelivery.attributes.filter(_.serviceDeliveryAttributeTypeCode.equals(Some("BIOMETRIC_CASE_TYPE"))) should have size 1
    val attributeValueRefDataElement = serviceDelivery.attributes.filter(_.serviceDeliveryAttributeTypeCode.equals(Some("BIOMETRIC_CASE_TYPE"))).head.attributeValueRefDataElement
    attributeValueRefDataElement.map(_.code) should equal(Some("BRP"))
  }

  test("Service delivery serialisation should accept Person involvement") {
    val serviceDeliveryJson = scala.io.Source.fromFile("src/test/resources/json/ServiceDelivery-MIBI-1161.json").mkString

    val serviceDelivery = parse(serviceDeliveryJson).extract[ServiceDelivery]

    serviceDelivery.involvements should have size 2
    serviceDelivery.involvements.filter(_.person.isDefined) should have size 1
    inside(serviceDelivery.involvements.map(_.person).head) {
      case Some(uk.gov..mi.model.servicedelivery.Person(personId, recentBiographics, _,_, _)) =>
        personId should equal("CRS_NAMED_PERSON_50_323756")
        recentBiographics shouldBe empty
    }
  }
}
