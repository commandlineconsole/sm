package uk.gov..mi.stream.servicedelivery

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Inside, Matchers}
import uk.gov..mi.model.HubServiceDeliveryInvolvedPerson
import uk.gov..mi.stream.HashHelper.emptyHash

@RunWith(classOf[JUnitRunner])
class InvolvedPersonHubTransformerTest extends FlatSpec with Matchers with Inside {

  "Service Delivery Involvement" should "have direct involvement " in {

    val serviceDelivery = ServiceDeliveryHelper.serviceDelivery("RandomId")

    val involvedPersons: List[HubServiceDeliveryInvolvedPerson] = InvolvedPersonHubTransformer.involvedPersonHub("messageId", serviceDelivery, System.currentTimeMillis())

    involvedPersons should have size serviceDelivery.involvements.size

    involvedPersons.head.message_id should equal("messageId")
    involvedPersons.head.involved_person_hk should not equal emptyHash
    involvedPersons.head.srvc_dlvry_handle_id should equal(serviceDelivery.internalHandle.interface_identifier)
    involvedPersons.head.involved_person_id should equal(serviceDelivery.involvements.sorted.head.person.map(_.personId))

  }

}
