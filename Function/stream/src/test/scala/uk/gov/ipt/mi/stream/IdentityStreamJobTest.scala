package uk.gov..mi.stream

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
import uk.gov..mi.model._
import uk.gov..mi.stream.HashHelper.emptyHash
import uk.gov..mi.stream.identity.IdentityHelper._
import uk.gov..mi.{stream, JsonFileFilter, PartitionedFilter}

import scala.collection.mutable
import scala.io.Source

@RunWith(classOf[JUnitRunner])
class IdentityStreamJobTest extends FlatSpec
  with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually {

  implicit val formats = DefaultFormats
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)))


  "Stream identity" should "write hub_identity to respective directories and parititons" in {
    Given("streaming context is initalised")
    val identities = mutable.Queue[RDD[(String, Identity, Long)]]()

    val hubIdentityDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "hub_identity")
    val sIdentityInfoDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_info")
    val sIdentityBiometricInfoDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_biometric_info")
    val sIdentityReferenceDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_reference")
    val sIdentityConditionDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_condition")
    val sIdentityBiographicsDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_biographics")
    val sIdentityDescrorsDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_descrors")
    val sIdentityMediasDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_medias")
    val lnkIdentityPersonDirectory = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_identity_medias")

    println(s"Created identity-hub-directory [${hubIdentityDirectory.toUri.toString}]")
    println(s"Created satellite-identity-info-directory [${sIdentityInfoDirectory.toUri.toString}]")
    println(s"Created satellite-identity-biometric-info-directory [${sIdentityBiometricInfoDirectory.toUri.toString}]")
    println(s"Created satellite-reference-directory [${sIdentityReferenceDirectory.toUri.toString}]")
    println(s"Created satellite-condition-directory [${sIdentityConditionDirectory.toUri.toString}]")
    println(s"Created satellite-biographics-directory [${sIdentityBiographicsDirectory.toUri.toString}]")
    println(s"Created satellite-descritpros-directory [${sIdentityDescrorsDirectory.toUri.toString}]")
    println(s"Created satellite-medias-directory [${sIdentityMediasDirectory.toUri.toString}]")
    println(s"Created link-person-directory [${lnkIdentityPersonDirectory.toUri.toString}]")

    val identityStreamConfig = MIStreamIdentityConfig("identityTopic",
      hubIdentityPath = hubIdentityDirectory.toUri.toString,
      sIdentityInfoPath = sIdentityInfoDirectory.toUri.toString,
      sIdentityBiometricInfoPath = sIdentityBiometricInfoDirectory.toUri.toString,
      sIdentityReferencePath = sIdentityReferenceDirectory.toUri.toString,
      sIdentityConditionPath = sIdentityConditionDirectory.toUri.toString,
      sIdentityBiographicsPath = sIdentityBiographicsDirectory.toUri.toString,
      sIdentityDescrorsPath = sIdentityDescrorsDirectory.toUri.toString,
      sIdentityMediasPath = sIdentityMediasDirectory.toUri.toString,
      sIdentityLinkPersonPath = lnkIdentityPersonDirectory.toUri.toString
    )

    val identityStreamJob = new IdentityStreamJob()

    val queueStream: InputDStream[(String, Identity, Long)] = ssc.queueStream(identities)

    identityStreamJob.start(queueStream, identityStreamConfig)

    ssc.start()

    When("a new identity is queued")

    val currentTimeMillis: Long = System.currentTimeMillis()
    val time = new DateTime(currentTimeMillis, DateTimeZone.UTC)

    val expectedIdentity: Identity = createIdentity("john_internal_id", "john_external_id")

    val kafkamessageid1 = "kafkaMessageId1"
    identities += spark.sparkContext.makeRDD(Seq(
      (kafkamessageid1,
        expectedIdentity,
        currentTimeMillis)
    ))

    Then("write identity to file .json extension")
    advanceClockOneBatch()
    eventually {
      hubIdentityDirectory.toFile.list().toList should have size 1
      sIdentityInfoDirectory.toFile.list().toList should have size 1
      sIdentityBiometricInfoDirectory.toFile.list().toList should have size 1
      sIdentityReferenceDirectory.toFile.list().toList should have size 1
      sIdentityConditionDirectory.toFile.list().toList should have size 1
      sIdentityBiographicsDirectory.toFile.list().toList should have size 1
      sIdentityDescrorsDirectory.toFile.list().toList should have size 1
      sIdentityMediasDirectory.toFile.list().toList should have size 1
      lnkIdentityPersonDirectory.toFile.list().toList should have size 1
    }

    Then("HubIdentity - check that .json file contains correct messageId and is created in correct partition")
    val hubPartitionedPath = hubIdentityDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(hubPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val hubIdentityFiles = hubPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualHubIdentity: HubIdentity = hubIdentityFiles.flatMap(hubIdentityFile => Source.fromFile(hubIdentityFile).getLines.toList.map(hubIdentityJson => parse(hubIdentityJson).extract[HubIdentity])).head

    actualHubIdentity.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)

    Then("SIdentityInfo = check that .json file contains correct fields mapped")

    val sidentityPartitionedPath = sIdentityInfoDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityPartitionedPath.toFile().list().toList should contain("_SUCCESS"))

    val sIdentityInfoFiles = sidentityPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualSIdentityInfo: SIdentityInfo = sIdentityInfoFiles.flatMap(sIdentityInfoFile => Source.fromFile(sIdentityInfoFile).getLines.toList.map(sIdentityInfoJson => parse(sIdentityInfoJson).extract[SIdentityInfo])).head

    actualSIdentityInfo.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)
    actualSIdentityInfo.identity_hk should not equal emptyHash

    Then("SIdentityBiometricInfo = check that .json file contains correct fields mapped")

    val sidentityBiometricInfoPartitionedPath = sIdentityBiometricInfoDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityBiometricInfoPartitionedPath.toFile().list().toList should contain("_SUCCESS"))

    val sidentityBiometricInfoFiles = sidentityBiometricInfoPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualBiometrics: List[SIdentityBiometricInfo] = sidentityBiometricInfoFiles.flatMap(sidentityBiometricInfoFile => Source
      .fromFile(sidentityBiometricInfoFile).getLines.toList
      .map(sIdentityBiometricInfoJson => parse(sIdentityBiometricInfoJson).extract[SIdentityBiometricInfo]))
      .sortBy(_.rec_seqno)


    actualBiometrics should have size 2


    actualBiometrics.head.message_id should equal(kafkamessageid1)
    actualBiometrics.head.identity_hk should not equal emptyHash
    actualBiometrics.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualBiometrics.head.record_source should equal("")
    actualBiometrics.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)
    Some(actualBiometrics.head.biometric_handle_id) should equal(expectedIdentity.biometrics.map(_.toList.sorted.head.internalHandle.interface_identifier))
    actualBiometrics.head.rec_seqno should equal(0)
    actualBiometrics.head.biometric_handle_visibility should equal(expectedIdentity.internal_handle.visibility_marker)
    actualBiometrics.head.biometric_ext_handle_value should equal(expectedIdentity.biometrics.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
    actualBiometrics.head.biometric_ext_handle_space should equal(expectedIdentity.biometrics.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
    actualBiometrics.head.biometric_nature should equal(expectedIdentity.biometrics.flatMap(_.toList.sorted.head.biometricNature))
    actualBiometrics.head.biometric_type_cd should equal(expectedIdentity.biometrics.flatMap(_.toList.sorted.head.biometricReferenceType))
    actualBiometrics.head.biometric_value should equal(expectedIdentity.biometrics.flatMap(_.toList.sorted.head.biometricReferenceValue))

    Then("SIdentityReference check that .json file contains correct fields mapped")

    val sidentityReferencePartitionedPath = sIdentityReferenceDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityReferencePartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sIdentityReferenceFiles = sidentityReferencePartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualReferences: List[SIdentityReference] = sIdentityReferenceFiles.flatMap(sIdentityReferenceFile => Source.fromFile(sIdentityReferenceFile).getLines().toList.map(sIdentityReferenceJson => parse(sIdentityReferenceJson).extract[SIdentityReference])).sortBy(_.rec_seqno)

    actualReferences should have size 2

    actualReferences.head.message_id should equal(kafkamessageid1)
    actualReferences.head.identity_hk should not equal emptyHash
    actualReferences.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualReferences.head.record_source should equal("")
    actualReferences.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)
    Some(actualReferences.head.reference_handle_id) should equal(expectedIdentity.references.map(_.toList.sorted.head.internalHandle.interface_identifier))
    actualReferences.head.rec_seqno should equal(0)
    actualReferences.head.reference_visibility should equal(expectedIdentity.internal_handle.visibility_marker)
    actualReferences.head.reference_ext_handle_value should equal(expectedIdentity.references.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
    actualReferences.head.reference_ext_handle_space should equal(expectedIdentity.references.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
    actualReferences.head.reference_type_cd should equal(expectedIdentity.references.flatMap(_.toList.sorted.head.referenceType.flatMap(_.code)))
    actualReferences.head.reference_value should equal(expectedIdentity.references.flatMap(_.toList.sorted.head.referenceValue))

    Then("SIdentityCondition check that .json file contains correct fields mapped")

    val sidentityConditionPartitionPath = sIdentityConditionDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityConditionPartitionPath.toFile.list.toList should contain("_SUCCESS"))

    val sIdentityConditionFiles = sidentityConditionPartitionPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualConditions: List[SIdentityCondition] = sIdentityConditionFiles.flatMap(sIdentityConditionFile => Source.fromFile(sIdentityConditionFile).getLines.toList.map(sIdentityConditionJson => parse(sIdentityConditionJson).extract[SIdentityCondition])).sortBy(_.rec_seqno)

    actualConditions should have size 2

    actualConditions.head.message_id should equal(kafkamessageid1)
    actualConditions.head.identity_hk should not equal emptyHash
    actualConditions.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualConditions.head.record_source should equal("")
    actualConditions.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)
    Some(actualConditions.head.condition_handle_id) should equal(expectedIdentity.conditions.map(_.toList.sorted.head.internalHandle.interface_identifier))
    actualConditions.head.rec_seqno should equal(0)
    actualConditions.head.condition_handle_visibility should equal(expectedIdentity.internal_handle.visibility_marker)
    actualConditions.head.condition_ext_handle_value should equal(expectedIdentity.conditions.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_value)))
    actualConditions.head.condition_ext_handle_space should equal(expectedIdentity.conditions.flatMap(_.toList.sorted.head.externalHandle.flatMap(_.handle_space)))
    actualConditions.head.condition_type_cd should equal(expectedIdentity.conditions.flatMap(_.toList.sorted.head.conditionType.flatMap(_.code)))
    actualConditions.head.condition_note should equal(expectedIdentity.conditions.flatMap(_.toList.sorted.head.conditionNote))

    Then("SIdentityBiographics check that .json file contains correct fields mapped")

    val sidentityBiographicsPartitionPath = sIdentityBiographicsDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityBiographicsPartitionPath.toFile.list().toList should contain("_SUCCESS"))

    val sIdentityBiographicsFiles = sidentityBiographicsPartitionPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualBiographics: List[SIdentityBiographics] = sIdentityBiographicsFiles.flatMap(sIdentityBiographicsFile => Source.fromFile(sIdentityBiographicsFile).getLines.toList.map(sIdentityBiographicsJson => parse(sIdentityBiographicsJson).extract[SIdentityBiographics])).sortBy(_.biographic_key)

    actualBiographics should have size 2

    actualBiographics.head.message_id should equal(kafkamessageid1)

    actualBiographics.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualBiographics.head.identity_hk should not equal emptyHash
    actualBiographics.head.record_source should equal("")
    actualBiographics.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)

    Some(actualBiographics.head.biog_set_handle_id) should equal(expectedIdentity.biographic_sets.map(_.toList.sorted.head.internal_handle.interface_identifier))
    actualBiographics.head.biog_set_handle_visibility should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.internal_handle.visibility_marker))
    actualBiographics.head.biog_set_ext_handle_space should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.external_handle.flatMap(_.handle_space)))
    actualBiographics.head.biog_set_ext_handle_value should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.external_handle.flatMap(_.handle_value)))
    actualBiographics.head.biog_set_purpose_cd should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.biographic_set_purpose.flatMap(_.code)))
    actualBiographics.head.biog_set_created_by should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.created_by))
    actualBiographics.head.biog_set_created_datetime should equal(expectedIdentity.biographic_sets.flatMap(_.toList.sorted.head.created))

    val (expectedBiogKey, expectedBiographic) = expectedIdentity.biographic_sets.map(_.toList.sorted.head.biographics.toList.sortBy(_._1).head).get
    actualBiographics.head.biographic_handle_id should equal(Some(expectedBiographic.internal_handle.interface_identifier))
    actualBiographics.head.biographic_rec_seqno should equal(Some(0))
    actualBiographics.head.biographic_handle_visibility should equal(expectedBiographic.internal_handle.visibility_marker)
    actualBiographics.head.biographic_key should equal(Some(expectedBiogKey))
    actualBiographics.head.biographic_type_cd should equal(expectedBiographic.`type`)
    actualBiographics.head.biographic_value should equal(expectedBiographic.value)
    actualBiographics.head.biographic_value_type_cd should equal(expectedBiographic.value_type)

    Then("SIdentityDescrions check that .json file contains correct fields mapped")

    val sidentityDescrorsPartitionPath = sIdentityDescrorsDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityDescrorsPartitionPath.toFile.list().toList should contain("_SUCCESS"))

    val sIdentityDescrors = sidentityDescrorsPartitionPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualDescrors: List[SIdentityDescrors] = sIdentityDescrors.flatMap(sIdentityDescror => Source.fromFile(sIdentityDescror).getLines.toList.map(sIdentityDescrorsJson => parse(sIdentityDescrorsJson).extract[SIdentityDescrors])).sortBy(_.descr_rec_seqno)

    actualDescrors should have size 2
    actualDescrors.head.message_id should equal(kafkamessageid1)
    actualDescrors.head.identity_hk should not equal emptyHash


    actualDescrors.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualDescrors.head.record_source should equal("")
    actualDescrors.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)

    Some(actualDescrors.head.descr_set_handle_id) should equal(expectedIdentity.descrions.map(_.toList.sorted.head.internalHandle.interface_identifier))

    Then("SIdentityMedia check that .json file contains correct fields mapped")

    val sidentityMediaPartitionPath = sIdentityMediasDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sidentityMediaPartitionPath.toFile.list.toList should contain ("_SUCCESS"))

    val sIdentityMedias = sidentityMediaPartitionPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualMedias: List[SIdentityMedia] = sIdentityMedias.flatMap(sIdentityMedia => Source.fromFile(sIdentityMedia).getLines.toList.map(sIdentityMediasJson => parse(sIdentityMediasJson).extract[SIdentityMedia])).sortBy(_.media_rec_seqno)

    actualMedias should have size 2

    actualMedias.head.message_id should equal(kafkamessageid1)

    actualMedias.head.bsn_event_datetime should equal(expectedIdentity.created)
    actualMedias.head.record_source should equal("")
    actualMedias.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)

    val expectedMediaSet = expectedIdentity.identity_media_sets.map(_.toList.sorted.head).get
    actualMedias.head.media_set_handle_id should equal(expectedMediaSet.internalHandle.interface_identifier)
    actualMedias.head.media_set_rec_hash_value should not equal emptyHash
    actualMedias.head.media_set_handle_visibility should equal(expectedMediaSet.internalHandle.visibility_marker)
    actualMedias.head.media_set_ext_handle_value should equal(expectedMediaSet.externalHandle.flatMap(_.handle_value))
    actualMedias.head.media_set_ext_handle_space should equal(expectedMediaSet.externalHandle.flatMap(_.handle_space))
    actualMedias.head.media_set_created_by should equal(expectedMediaSet.createdBy)
    actualMedias.head.media_set_created_datetime should equal(expectedMediaSet.created)
    actualMedias.head.media_set_agg_hash should not equal emptyHash
    actualMedias.head.lnk_idntty_media_set_hk should not equal emptyHash

    val expectedMedia = expectedMediaSet.identityMedias.map(_.toList.sorted.head).get
    actualMedias.head.media_handle_id should equal(Some(expectedMedia.internalHandle.interface_identifier))
    actualMedias.head.media_rec_seqno should equal(Some(0))
    actualMedias.head.media_rec_hash_value should not equal emptyHash
    actualMedias.head.media_handle_visibility should equal(expectedMedia.internalHandle.visibility_marker)
    actualMedias.head.media_type_cd should equal(expectedMedia.identityMediaType)
    actualMedias.head.media_file_handle_id should equal(expectedMedia.fileHandle.map(_.interface_identifier))

    Then("LinkIdentityPerson check that .json file contains correct fields mapped")

    val linkIdentityPersonPath = lnkIdentityPersonDirectory.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(linkIdentityPersonPath.toFile.list.toList should contain("_SUCCESS"))

    val linkIdentities = linkIdentityPersonPath.toFile.listFiles(new PartitionedFilter()).toList

    val linkIdentityPerson = linkIdentities.flatMap(linkIdentity => Source.fromFile(linkIdentity).getLines.toList.map(linkIdentityJson => parse(linkIdentityJson).extract[LinkIdentityPerson]))

    linkIdentityPerson should have size 1

    linkIdentityPerson.head.message_id should equal(kafkamessageid1)
    linkIdentityPerson.head.identity_hk should not equal emptyHash
    linkIdentityPerson.head.record_source should equal("")
    linkIdentityPerson.head.identity_handle_id should equal(expectedIdentity.internal_handle.interface_identifier)
    Some(linkIdentityPerson.head.person_handle_id) should equal(expectedIdentity.containing_person_handle.map(_.interface_identifier))


    deleteFile(hubIdentityDirectory.toFile)
    deleteFile(sIdentityInfoDirectory.toFile)
    deleteFile(sIdentityBiometricInfoDirectory.toFile)
    deleteFile(sIdentityReferenceDirectory.toFile)
    deleteFile(sIdentityConditionDirectory.toFile)
    deleteFile(sIdentityBiographicsDirectory.toFile)
    deleteFile(sIdentityDescrorsDirectory.toFile)
    deleteFile(sIdentityMediasDirectory.toFile)
    deleteFile(lnkIdentityPersonDirectory.toFile)
  }
}


