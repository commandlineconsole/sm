package uk.gov.ipt.mi.stream

import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import uk.gov.ipt.mi.PartitionedFilter
import uk.gov.ipt.mi.model._
import uk.gov.ipt.mi.stream.HashHelper.emptyHash

import scala.collection.mutable
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class PersonStreamJobTest extends FlatSpec
  with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  implicit val formats = DefaultFormats
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)))

  "Stream person" should "write hub_person, change_log, visibility and identities to respective dir and files" in {
    Given("streaming context is initialised")
    val persons = mutable.Queue[RDD[(String, Person, Long)]]()

    val hubPersonDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "hub_person")
    val personChangeLogDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "person_change_log")
    val personVisibilityDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "person_visibility")
    val lnkPersonIdentityDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "lnk_person_identity")

    println(s"Created person-hub-directory [${hubPersonDirectory.toUri.toString}]")
    println(s"Created person-change-log-directory [${personChangeLogDirectory.toUri.toString}]")
    println(s"Created person-visibility-directory [${personVisibilityDirectory.toUri.toString}]")
    println(s"Created lnk-person-identity-directory [${lnkPersonIdentityDirectory.toUri.toString}]")

    val personStreamJob = new PersonStreamJob()

    val queueStream: InputDStream[(String, Person, Long)] = ssc.queueStream(persons)

    personStreamJob.streamHubPerson(queueStream, hubPersonDirectory.toUri.toString)
    personStreamJob.streamPersonChangeLog(queueStream, personChangeLogDirectory.toUri.toString)
    personStreamJob.streamPersonVisibility(queueStream, personVisibilityDirectory.toUri.toString)
    personStreamJob.streamLinkPerson(queueStream, lnkPersonIdentityDirectory.toUri.toString)

    hubPersonDirectory.toFile.list() shouldBe empty

    ssc.start()

    When("add one person to queued")
    val currentTimeMillis: Long = System.currentTimeMillis()
    val time = new DateTime(currentTimeMillis, DateTimeZone.UTC)
    //create a person with two identities attached.
    val expectedPerson: Person = createPerson("person1", Set(createInternalHandle("identity1"), createInternalHandle("identity2")))
    persons += spark.sparkContext.makeRDD(Seq(
      ("messageId1",
        expectedPerson,
        currentTimeMillis))
    )

    Then("write person to file")
    advanceClockOneBatch()
    eventually {
      hubPersonDirectory.toFile.list().toList should have size 1
      personChangeLogDirectory.toFile.list().toList should have size 1
      personVisibilityDirectory.toFile.list().toList should have size 1
      lnkPersonIdentityDirectory.toFile.list().toList should have size 1
    }

    Then("HubPerson - Check that .json file contains correct messageId and is created in correct partition")
    val hubPartitionedPath = hubPersonDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually {
      hubPartitionedPath.toFile.list().toList should contain("_SUCCESS")
    }

    val hubPersonFiles = hubPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualHubPerson = hubPersonFiles.flatMap(hubPersonFile => Source.fromFile(hubPersonFile).getLines.toList.map(hubPersonJson => parse(hubPersonJson).extract[HubPerson])).head

    actualHubPerson.person_handle_id should equal(expectedPerson.internal_handle.interface_identifier)

    Then("Person - check visibility .json file contains correct person visibility")
    val visibilityPartitionedPath = personVisibilityDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually {
      visibilityPartitionedPath.toFile.list().toList should contain("_SUCCESS")
    }

    val personVisibilityFiles = visibilityPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualPersonVisibility = personVisibilityFiles.flatMap(personVisibilityFile => Source.fromFile(personVisibilityFile).getLines.toList.map(personVisibilityJson => parse(personVisibilityJson).extract[PersonVisibility])).head

    actualPersonVisibility.person_handle_id should equal(expectedPerson.internal_handle.interface_identifier)
    actualPersonVisibility.person_handle_visibility should equal(expectedPerson.internal_handle.visibility_marker)
    actualPersonVisibility.person_hk should not equal emptyHash

    Then("Person - check two identities are created in .json for link person and identities")
    val lnkPersonIdentityPartitionedPath = lnkPersonIdentityDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually {
      lnkPersonIdentityPartitionedPath.toFile.list().toList should contain("_SUCCESS")
    }

    val lnkPersonIdentityFiles = lnkPersonIdentityPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val identities = lnkPersonIdentityFiles.flatMap(lnkPersonIdentityFile => Source.fromFile(lnkPersonIdentityFile).getLines.toList.map(personLinkJson => parse(personLinkJson).extract[LinkPerson]))

    identities should have size 2

    all(identities) should (have('identity_handle_id ("identity1")) or have('identity_handle_id ("identity2")))

    //cleanup
    deleteFile(hubPersonDirectory.toFile)
    deleteFile(personChangeLogDirectory.toFile)
    deleteFile(personVisibilityDirectory.toFile)
    deleteFile(lnkPersonIdentityDirectory.toFile)
  }

  def createPerson(interfaceIdentifier: String): Person = {
    createPerson(interfaceIdentifier, Set())
  }

  def createPerson(interfaceIdentifier: String, identities: Set[InternalHandle]): Person = {
    Person(internal_handle = createInternalHandle(interfaceIdentifier),
      person_space = Some("IPT"),
      created = Some("createdDate"),
      created_by = None,
      identity_handles = identities)
  }

  def createInternalHandle(identifier: String) = InternalHandle(identifier, "uniqueId", Some("DEFAULT"))
}
