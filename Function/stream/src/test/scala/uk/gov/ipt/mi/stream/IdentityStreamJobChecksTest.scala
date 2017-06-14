package uk.gov.ipt.mi.stream

import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.joda.time.{DateTime, DateTimeZone}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Seconds, Span}
import uk.gov.ipt.mi.PartitionedFilter
import uk.gov.ipt.mi.model.{Identity, SIdentityBiographics}

import scala.collection.mutable
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class IdentityStreamJobChecksTest extends FlatSpec
  with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  implicit val formats = DefaultFormats
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)))

  "Stream identity" should "write biographics without duplicates" in {

    val identities = mutable.Queue[RDD[(String, Identity, Long)]]()

    val sIdentityBiographicsDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_biographics")

    println(s"Created satellite-biographics-directory [${sIdentityBiographicsDirectory.toUri.toString}]")

    val identityStreamJob = new IdentityStreamJob()

    val queueStream: InputDStream[(String, Identity, Long)] = ssc.queueStream(identities)

    identityStreamJob.streamSIdentityBiographics(queueStream, sIdentityBiographicsDirectory.toUri.toString)

    ssc.start()

    When("append 3 identites")

    val expectedIdentites: List[Identity] = scala.io.Source.fromFile("src/test/resources/json/identity-MIBI-451-duplicates.json").getLines.map(identityJson => parse(identityJson).extract[Identity]).toList

    identities += spark.sparkContext.makeRDD(expectedIdentites.map(("kafkaMessageId", _, System.currentTimeMillis())))

    Then("Write biographics to file")
    advanceClockOneBatch()
    eventually {
      sIdentityBiographicsDirectory.toFile.list().toList should have size 1
    }

    Then("check biographics expected size")

    val currentTimeMillis: Long = System.currentTimeMillis()
    val time = new DateTime(currentTimeMillis, DateTimeZone.UTC)

    val sidentityBiographicsPartitionPath = sIdentityBiographicsDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityBiographicsPartitionPath.toFile.list().toList should contain("_SUCCESS"))

    val sIdentityBiographicsFiles = sidentityBiographicsPartitionPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualBiographics: List[SIdentityBiographics] = sIdentityBiographicsFiles.flatMap(sIdentityBiographicsFile => Source.fromFile(sIdentityBiographicsFile).getLines.toList.map(sIdentityBiographicsJson => parse(sIdentityBiographicsJson).extract[SIdentityBiographics])).sortBy(_.biographic_key)

    actualBiographics should have size expectedIdentites.foldLeft(0)((size, identity) => size + identity.biographic_sets.map(biogSet => biogSet.foldLeft(0)((setSize, set) => setSize + set.biographics.size)).getOrElse(0))

    deleteFile(sIdentityBiographicsDirectory.toFile)

  }

}
