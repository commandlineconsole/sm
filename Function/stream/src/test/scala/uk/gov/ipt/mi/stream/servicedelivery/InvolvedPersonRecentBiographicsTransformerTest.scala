package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.{Inside, Matchers, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov..mi.stream.HashHelper

@RunWith(classOf[JUnitRunner])
class InvolvedPersonRecentBiographicsTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement Person " should "have recent biographics " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")
    val recentBiographics = SInvolvedPersonRecentBiographicsTransformer.involvedPersonRecentBiographics("messageId", serviceDelivery, System.currentTimeMillis())

    val expectedRecentBiographicsSize = serviceDelivery.involvements.map(involvement => {
      involvement.person.map(_.recentBiographics.size).getOrElse(0)
    }).sum

    recentBiographics should have size expectedRecentBiographicsSize

    val firstBiographics = recentBiographics.head
    firstBiographics.message_id should equal("messageId")
    firstBiographics.rec_hash_value should not equal HashHelper.emptyHash
    firstBiographics.involved_person_hk should not equal HashHelper.emptyHash
    firstBiographics.involved_person_id should equal(serviceDelivery.involvements.sorted.head.person.map(_.personId))
    firstBiographics.biographic_handle_id should equal(serviceDelivery.involvements.sorted.head.person.flatMap(_.recentBiographics.toList.sorted.head.internalHandle.map(_.interface_identifier)))
    firstBiographics.biographic_type_cd should equal(serviceDelivery.involvements.sorted.head.person.map(_.recentBiographics.toList.sorted.head.biographicType).get)
    firstBiographics.biographic_value should equal(serviceDelivery.involvements.sorted.head.person.map(_.recentBiographics.toList.sorted.head.biographicValue).get)
    firstBiographics.rec_seqno should equal(0)
    firstBiographics.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)







  }

}
