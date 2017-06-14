package uk.gov.ipt.mi.dw.person

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import uk.gov.ipt.mi.SparkSpec
import uk.gov.ipt.mi.model.HubPerson
import uk.gov.ipt.mi.dw.{DbConfig, DbHelper, MIPersonVaultConfig}
import java.io.{File, FileInputStream}
import java.util.Properties

import org.junit.Ignore

@RunWith(classOf[JUnitRunner])
@Ignore
class PersonVaultJobTest extends FlatSpec with SparkSpec with GivenWhenThen with Matchers with Eventually {

  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)))


  "PersonVaultJob" should "insert all incoming hubs if current hub source is empty" in {
    Given("Incoming person read from JSON")
    val incomingPerson = spark.createDataFrame(Seq(HubPerson(
      "messageId1", Some("event_datetime"), "LandingTime",
      "hash1", "IPT", "handle1")))

    When("current person is empty")
    val currentPersonHub = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], classOf[TableHubPerson])

    val personVaultJob = new PersonVaultJob("batchId", "aTimestampStr", spark)

    Then("person hub merged vault contains all of incoming")
	
/*	val mergedDF = personVaultJob.fnFilterHash(incomingPerson,dbConfig,personVaultConfig)

    val rows = mergedDF.collect()

    mergedDF.show()

	rows should equal(Array(Row("hash1", "event_datetime", "IPT", "handle1", "batchId", "aTimestampStr")))	
*/	
  }

  case class TableHubPerson(person_hk: String, record_source: String, person_handle_id: String)

}
