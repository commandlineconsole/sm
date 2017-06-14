package uk.gov.ipt.mi.stream

import java.nio.file.Files

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.joda.time.{DateTimeZone, DateTime}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Inside, Matchers, GivenWhenThen, FlatSpec}
import org.scalatest.junit.JUnitRunner
import uk.gov.ipt.mi.PartitionedFilter
import uk.gov.ipt.mi.model._
import uk.gov.ipt.mi.model.servicedelivery.ServiceDelivery
import uk.gov.ipt.mi.stream.HashHelper.emptyHash
import uk.gov.ipt.mi.stream.servicedelivery.ServiceDeliveryHelper

import scala.collection.mutable
import scala.io.Source


@RunWith(classOf[JUnitRunner])
class InvolvementStreamJobTest extends FlatSpec
with SparkStreamingSpec with GivenWhenThen with Matchers with Eventually with Inside {

  implicit val formats = DefaultFormats
  implicit override val patienceConfig = PatienceConfig(timeout = scaled(Span(60, Seconds)))

  "Stream service delivery" should "write hub, satellite info " in {
    Given("streaming context is initialised")
    val serviceDeliveries = mutable.Queue[RDD[(String, ServiceDelivery, Long)]]()

    val hubInvolvementPath = Files.createTempDirectory(this.getClass.getSimpleName + "hub_involvement")
    val sInvolvementInfoPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involvement_info")
    val hubInvolvedPersonPath = Files.createTempDirectory(this.getClass.getSimpleName + "hub_involved_person")
    val sInvolvedPersonInfoPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involved_person_info")
    val sInvolvedPersonRecentBiographicsPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involved_person_recent_biographics")
    val lInvolvementPartyPath = Files.createTempDirectory(this.getClass.getSimpleName + "link_involvement_party")
    val hubInvolvementDocAttachmentsPath = Files.createTempDirectory(this.getClass.getSimpleName + "hub_involvement_doc_attachments")
    val sDocAttachmentInfoPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_doc_attachmen_info")
    val hubInvolvementDocAttachCorrespondencePath = Files.createTempDirectory(this.getClass.getSimpleName + "hub_involvement_doc_attachment_correspondences")
    val sInvolvementDocAttachCorrespondenceInfoPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involvement_doc_attachment_correspondence_infos")
    val sInvolvementPOAddressesPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involvement_po_addresses")
    val sInvolvementElAddressesPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involvement_el_addresses")
    val sInvolvementNotesPath = Files.createTempDirectory(this.getClass.getSimpleName + "satellite_involvement_notes")

    println(s"Created hub_service_delivery [${hubInvolvementPath.toUri.toString}]")
    println(s"Created satellite_service_delivery_info [${sInvolvementInfoPath.toUri.toString}]")
    println(s"Created hub_involved_person [${hubInvolvedPersonPath.toUri.toString}]")
    println(s"Created satellite_involved_person_info [${sInvolvedPersonInfoPath.toUri.toString}]")
    println(s"Created satellite_involved_person_recent_biographics [${sInvolvedPersonRecentBiographicsPath.toUri.toString}]")
    println(s"Created link_involvement_party [${lInvolvementPartyPath.toUri.toString}]")
    println(s"Created hub_involvement_doc_attachments [${hubInvolvementDocAttachmentsPath.toUri.toString}]")
    println(s"Created satellite_doc_attachment_info [${sDocAttachmentInfoPath.toUri.toString}]")
    println(s"Created hub_involvement_doc_attachment_correspondences [${hubInvolvementDocAttachCorrespondencePath.toUri.toString}]")
    println(s"Created satellite_involvement_doc_attachment_correspondence_infos [${sInvolvementDocAttachCorrespondenceInfoPath.toUri.toString}]")
    println(s"Created satellite_involvement_po_addresses [${sInvolvementPOAddressesPath.toUri.toString}]")
    println(s"Created satellite_involvement_el_addresses [${sInvolvementElAddressesPath.toUri.toString}]")
    println(s"Created satellite_involvement_notes [${sInvolvementNotesPath.toUri.toString}]")

    val serviceDeliveryStreamJob = new ServiceDeliveryInvolvementStreamJob()

    val queueStream: InputDStream[(String, ServiceDelivery, Long)] = ssc.queueStream(serviceDeliveries)

    val serviceDeliveryInvolvementConfig = new MIStreamServiceDeliveryInvlConfig(
      hubInvolvementPath.toUri.toString, sInvolvementInfoPath.toUri.toString, hubInvolvedPersonPath.toUri.toString,
      sInvolvedPersonInfoPath.toUri.toString, sInvolvedPersonRecentBiographicsPath.toUri.toString, lInvolvementPartyPath.toUri.toString,
      hubInvolvementDocAttachmentsPath.toUri.toString, sDocAttachmentInfoPath.toUri.toString, hubInvolvementDocAttachCorrespondencePath.toUri.toString,
      sInvolvementDocAttachCorrespondenceInfoPath.toUri.toString, sInvolvementPOAddressesPath.toUri.toString, sInvolvementElAddressesPath.toUri.toString,
      sInvolvementNotesPath.toUri.toString
    )

    serviceDeliveryStreamJob.start(queueStream, serviceDeliveryInvolvementConfig)

    ssc.start()

    When("add one service delivery to queue")
    val currentTimeMillis: Long = System.currentTimeMillis()
    val time = new DateTime(currentTimeMillis, DateTimeZone.UTC)

    val expectedSD = ServiceDeliveryHelper.serviceDelivery("serviceDelivery-1")

    serviceDeliveries += spark.sparkContext.makeRDD(Seq(("messageId1", expectedSD, currentTimeMillis)))

    Then("write service delivery to file")
    advanceClockOneBatch()

    eventually {
      hubInvolvementPath.toFile.list().toList should have size 1
      sInvolvementInfoPath.toFile.list().toList should have size 1
      hubInvolvedPersonPath.toFile.list().toList should have size 1
      sInvolvedPersonInfoPath.toFile.list().toList should have size 1
      sInvolvedPersonRecentBiographicsPath.toFile.list().toList should have size 1
      lInvolvementPartyPath.toFile.list().toList should have size 1
      hubInvolvementDocAttachmentsPath.toFile.list().toList should have size 1
      sInvolvementPOAddressesPath.toFile.list().toList should have size 1
      sInvolvementElAddressesPath.toFile.list().toList should have size 1
      sInvolvementNotesPath.toFile.list().toList should have size 1
    }

    Then("HubInvolvementInfo - check that hub contains correct values")
    val hubPartitionedPath = hubInvolvementPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(hubPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val hubServiceDeliveryInvolvementFiles = hubPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualHubServiceDeliveryInvolvement: HubServiceDeliveryInvolvement = hubServiceDeliveryInvolvementFiles.flatMap(
      hubServiceDeliveryInvolvementFile => Source.fromFile(hubServiceDeliveryInvolvementFile).getLines().toList.map(hubServiceDeliveryInvolvementJson => parse(hubServiceDeliveryInvolvementJson).extract[HubServiceDeliveryInvolvement])).head

    actualHubServiceDeliveryInvolvement.invlmnt_hk should not equal emptyHash
    actualHubServiceDeliveryInvolvement.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)

    Then("SServiceDeliveryInvolvementInfo - check that satellite contains correct values")

    val sSDInvolvementInfoPartitionedPath = sInvolvementInfoPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sSDInvolvementInfoPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sSDInvolvementInfoFiles = sSDInvolvementInfoPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSSDInvolvementInfo: List[SServiceDeliveryInvolvementInfo] = sSDInvolvementInfoFiles.flatMap(
      sSDInvolvementInfoFile => Source.fromFile(sSDInvolvementInfoFile).getLines().toList.map(sSDInfoJson => parse(sSDInfoJson).extract[SServiceDeliveryInvolvementInfo]))

    actualSSDInvolvementInfo should have size (expectedSD.involvements.size + expectedSD.indirectInvolvements.size)

    actualSSDInvolvementInfo(0).message_id should equal("messageId1")
    actualSSDInvolvementInfo(0).invlmnt_method_cd should equal("DIRECT")
    actualSSDInvolvementInfo(0).chlg_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(0).vis_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(0).invlmnt_hk should not equal emptyHash

    actualSSDInvolvementInfo(1).message_id should equal("messageId1")
    actualSSDInvolvementInfo(1).invlmnt_method_cd should equal("DIRECT")
    actualSSDInvolvementInfo(1).chlg_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(1).vis_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(1).invlmnt_hk should not equal emptyHash

    actualSSDInvolvementInfo(2).message_id should equal("messageId1")
    actualSSDInvolvementInfo(2).invlmnt_method_cd should equal("INDIRECT")
    actualSSDInvolvementInfo(2).chlg_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(2).vis_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(2).invlmnt_hk should not equal emptyHash

    actualSSDInvolvementInfo(3).message_id should equal("messageId1")
    actualSSDInvolvementInfo(3).invlmnt_method_cd should equal("INDIRECT")
    actualSSDInvolvementInfo(3).chlg_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(3).vis_rec_hash_value should not equal emptyHash
    actualSSDInvolvementInfo(3).invlmnt_hk should not equal emptyHash

    Then("HubInvolvedPerson - check that involved person hub contains correct values")
    val involvedPersonHubPartitionedPath = hubInvolvedPersonPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(involvedPersonHubPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val hubInvolvedPersonFiles = involvedPersonHubPartitionedPath.toFile.listFiles(new PartitionedFilter())

    val actualHubInvolvedPerson: HubServiceDeliveryInvolvedPerson = hubInvolvedPersonFiles.flatMap(
      hubInvolvedPersonFile => Source.fromFile(hubInvolvedPersonFile).getLines().toList.map(hubInvolvedPersonFileJson => parse(hubInvolvedPersonFileJson).extract[HubServiceDeliveryInvolvedPerson])).head

    actualHubInvolvedPerson.involved_person_hk should not equal emptyHash
    actualHubInvolvedPerson.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    actualHubInvolvedPerson.involved_person_id should equal(expectedSD.involvements.sorted.head.person.map(_.personId))

    Then("SInvolvedPersonInfo - check that the satellite contains correct values")

    val sInvolvedPersonInfoPartitionedPath = sInvolvedPersonInfoPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvedPersonInfoPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sSDsInvolvedPersonInfoFiles = sInvolvedPersonInfoPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSSDInvolvedPersonInfoList: List[SServiceDeliveryInvolvedPersonInfo] = sSDsInvolvedPersonInfoFiles.flatMap(
      sSDInvolvedPersonInfoFile => Source.fromFile(sSDInvolvedPersonInfoFile).getLines().toList.map(sSDInvovledPersonInfoJson => parse(sSDInvovledPersonInfoJson).extract[SServiceDeliveryInvolvedPersonInfo]))

    actualSSDInvolvedPersonInfoList should have size expectedSD.involvements.size

    val firstInvolvedPersonInfo = actualSSDInvolvedPersonInfoList.head
    firstInvolvedPersonInfo.message_id should equal("messageId1")
    firstInvolvedPersonInfo.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstInvolvedPersonInfo.default_biog_rec_hash_value should not equal emptyHash
    firstInvolvedPersonInfo.involved_person_hk should not equal emptyHash
    firstInvolvedPersonInfo.involved_person_id should equal(expectedSD.involvements.sorted.head.person.map(_.personId))

    Then("SInvolvedPersonRecentBiographics - check that the satellite contains correct values")

    val sInvolvedPersonRecentBiographicsPartitionedPath = sInvolvedPersonRecentBiographicsPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvedPersonRecentBiographicsPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sSDInvolvedPersonRecentBiographicsFiles = sInvolvedPersonRecentBiographicsPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSSDInvolvedPersonRecentBiographicsFilesList: List[SServiceDeliveryInvolvedPersonRecentBiographics] = sSDInvolvedPersonRecentBiographicsFiles.flatMap(
      sSDInvolvedPersonRecentBiographicsFile => Source.fromFile(sSDInvolvedPersonRecentBiographicsFile).getLines().toList.map(sSDInvolvedPersonRecentBiographicsJson => parse(sSDInvolvedPersonRecentBiographicsJson).extract[SServiceDeliveryInvolvedPersonRecentBiographics]))

    val expectedRecentBiographicsSize = expectedSD.involvements.map(involvement => {
      involvement.person.map(_.recentBiographics.size).getOrElse(0)
    }).sum

    actualSSDInvolvedPersonRecentBiographicsFilesList should have size expectedRecentBiographicsSize

    val firstInvolvedPersonRecentBiographics = actualSSDInvolvedPersonRecentBiographicsFilesList.head

    firstInvolvedPersonRecentBiographics.message_id should equal("messageId1")
    firstInvolvedPersonRecentBiographics.rec_hash_value should not equal HashHelper.emptyHash
    firstInvolvedPersonRecentBiographics.involved_person_hk should not equal HashHelper.emptyHash
    firstInvolvedPersonRecentBiographics.involved_person_id should equal(expectedSD.involvements.sorted.head.person.map(_.personId))
    firstInvolvedPersonRecentBiographics.biographic_handle_id should equal(expectedSD.involvements.sorted.head.person.flatMap(_.recentBiographics.toList.sorted.head.internalHandle.map(_.interface_identifier)))
    firstInvolvedPersonRecentBiographics.biographic_type_cd should equal(expectedSD.involvements.sorted.head.person.map(_.recentBiographics.toList.sorted.head.biographicType).get)
    firstInvolvedPersonRecentBiographics.biographic_value should equal(expectedSD.involvements.sorted.head.person.map(_.recentBiographics.toList.sorted.head.biographicValue).get)
    firstInvolvedPersonRecentBiographics.rec_seqno should equal(0)
    firstInvolvedPersonRecentBiographics.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)

    Then("LinkServiceDeliveryInvolvementParty - check that the link party info contains correct values")

    val lLinkServiceDeliveryInvolvementPartyPartitionedPath = lInvolvementPartyPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(lLinkServiceDeliveryInvolvementPartyPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val lSDInvolvementPartyFiles = lLinkServiceDeliveryInvolvementPartyPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actuallSDInvolvementPartyList: List[LinkServiceDeliveryInvolvementParty] = lSDInvolvementPartyFiles.flatMap(
      lSDInvolvementPartyFile => Source.fromFile(lSDInvolvementPartyFile).getLines().toList.map(lSDInvolvementPartyJson => parse(lSDInvolvementPartyJson).extract[LinkServiceDeliveryInvolvementParty]))

    actuallSDInvolvementPartyList should have size expectedSD.involvements.size + expectedSD.indirectInvolvements.size

    val firstInvolvementPartyLink = actuallSDInvolvementPartyList.filter(_.invlmnt_method_cd.equals("DIRECT")).head

    firstInvolvementPartyLink.lnk_srvcdlvry_party_invlmnt_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.srvc_dlvry_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.invlmnt_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.person_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.involved_person_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.identity_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.org_hk should not equal HashHelper.emptyHash
    firstInvolvementPartyLink.individual_hk should not equal HashHelper.emptyHash

    firstInvolvementPartyLink.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstInvolvementPartyLink.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)
    firstInvolvementPartyLink.person_handle_id should equal(expectedSD.involvements.sorted.head.personHandle.map(_.interface_identifier))
    firstInvolvementPartyLink.individual_handle_id should equal(Some(""))
    firstInvolvementPartyLink.involved_person_id should equal(expectedSD.involvements.sorted.head.person.map(_.personId))
    firstInvolvementPartyLink.org_handle_id should equal(expectedSD.involvements.sorted.head.organisationHandle.map(_.interface_identifier))
    firstInvolvementPartyLink.invlmnt_role_type_cd should equal(expectedSD.involvements.sorted.head.involvementRoleType.refDataValueCode)
    firstInvolvementPartyLink.invlmnt_role_subtype_cd should equal(expectedSD.involvements.sorted.head.involvementRoleSubType.map(_.refDataValueCode))

    val firstIndirectInvolvementPartyLink = actuallSDInvolvementPartyList.filter(_.invlmnt_method_cd.equals("INDIRECT")).head

    firstIndirectInvolvementPartyLink.lnk_srvcdlvry_party_invlmnt_hk should not equal HashHelper.emptyHash
    firstIndirectInvolvementPartyLink.srvc_dlvry_hk should not equal HashHelper.emptyHash
    firstIndirectInvolvementPartyLink.invlmnt_hk should not equal HashHelper.emptyHash
    firstIndirectInvolvementPartyLink.person_hk should equal(HashHelper.emptyHash)
    firstIndirectInvolvementPartyLink.involved_person_hk should equal(HashHelper.emptyHash)
    firstIndirectInvolvementPartyLink.identity_hk should equal(HashHelper.emptyHash)
    firstIndirectInvolvementPartyLink.org_hk should not equal HashHelper.emptyHash
    firstIndirectInvolvementPartyLink.individual_hk should not equal HashHelper.emptyHash

    firstIndirectInvolvementPartyLink.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstIndirectInvolvementPartyLink.invlmnt_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.internalHandle.interface_identifier)
    firstIndirectInvolvementPartyLink.person_handle_id should equal(Some(""))
    firstIndirectInvolvementPartyLink.individual_handle_id should equal(Some(expectedSD.indirectInvolvements.sorted.head.individualHandle.interface_identifier))
    firstIndirectInvolvementPartyLink.involved_person_id should equal(Some(""))
    firstIndirectInvolvementPartyLink.org_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.organisationHandle.map(_.interface_identifier))
    firstIndirectInvolvementPartyLink.invlmnt_role_type_cd should equal(expectedSD.indirectInvolvements.sorted.head.involvementRoleType.refDataValueCode)
    firstIndirectInvolvementPartyLink.invlmnt_role_subtype_cd should equal(expectedSD.indirectInvolvements.sorted.head.involvementRoleSubType.map(_.refDataValueCode))


    Then("HubInvolvementDocAttachment - check that the involvement doc attachments contain correct values")

    val hubInvolvementDocAttachmentsPartitionedPath = hubInvolvementDocAttachmentsPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(hubInvolvementDocAttachmentsPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val hubInvolvementDocAttachmentFiles = hubInvolvementDocAttachmentsPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actuallHubInvolvementDocAttachmentsList: List[HubDocAttachement] = hubInvolvementDocAttachmentFiles.flatMap(
      hubInvolvementDocAttachment => Source.fromFile(hubInvolvementDocAttachment).getLines().toList.map(hubInvolvementDocAttachmentJson => parse(hubInvolvementDocAttachmentJson).extract[HubDocAttachement]))

    actuallHubInvolvementDocAttachmentsList should have size expectedSD.involvements.map(_.documentAttachments.size).sum

    val firstDocAttachmentHub = actuallHubInvolvementDocAttachmentsList.head
    firstDocAttachmentHub.doc_atchmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentHub.doc_atchmnt_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentHub.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)

    Then("SServiceDeliveryDocAttachmentInfo - check that the involvement doc attachments info contain correct values")

    val sInvolvementDocAttachmentsInfoPartitionedPath = sDocAttachmentInfoPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvementDocAttachmentsInfoPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sInvolvementDocAttachmentInfoFiles = sInvolvementDocAttachmentsInfoPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSInvolvementDocAttachmentsInfoList: List[SServiceDeliveryDocAttachmentInfo] = sInvolvementDocAttachmentInfoFiles.flatMap(
      sInvolvementDocAttachmentInfo => Source.fromFile(sInvolvementDocAttachmentInfo).getLines().toList.map(sInvolvementDocAttachmentInfoJson => parse(sInvolvementDocAttachmentInfoJson).extract[SServiceDeliveryDocAttachmentInfo]))

    actualSInvolvementDocAttachmentsInfoList should have size expectedSD.involvements.map(_.documentAttachments.size).sum

    val firstsDocAttachmentInfo = actualSInvolvementDocAttachmentsInfoList.head

    firstsDocAttachmentInfo.main_rec_hash_value should not equal HashHelper.emptyHash
    firstsDocAttachmentInfo.chlg_rec_hash_value should not equal HashHelper.emptyHash
    firstsDocAttachmentInfo.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)
    firstsDocAttachmentInfo.doc_atchmnt_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstsDocAttachmentInfo.doc_atchmnt_doc_store_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.documentStoreId)
    firstsDocAttachmentInfo.doc_atchmnt_mime_type_cd should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.mimeType.refDataValueCode)
    firstsDocAttachmentInfo.doc_atchmnt_provider_cd should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.provider.refDataValueCode)
    firstsDocAttachmentInfo.doc_atchmnt_verified_flag should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.verified)
    firstsDocAttachmentInfo.doc_atchmnt_stage_cd should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.stageCode)


    Then("HubInvolvementDocAttachmentCorrespondence - check that the involvement doc attachments contain correct corespondences")

    val hubInvolvementDocAttachmentCorrespondencessPartitionedPath = hubInvolvementDocAttachCorrespondencePath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(hubInvolvementDocAttachmentCorrespondencessPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val hubInvolvementDocAttachmentCorrespondencesFiles = hubInvolvementDocAttachmentCorrespondencessPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualHubInvolvementDocAttachmentCorrespondencesList: List[HubCorrespondence] = hubInvolvementDocAttachmentCorrespondencesFiles.flatMap(
      hubInvolvementDocAttachmentCorrespondence => Source.fromFile(hubInvolvementDocAttachmentCorrespondence).getLines().toList.map(hubInvolvementDocAttachmentCorrespondenceJson => parse(hubInvolvementDocAttachmentCorrespondenceJson).extract[HubCorrespondence]))

    actualHubInvolvementDocAttachmentCorrespondencesList should have size expectedSD.involvements.flatMap(_.documentAttachments.flatMap(_.correspondences.map(_.size))).sum

    val firstDocAttachmentCorrespondenceHub = actualHubInvolvementDocAttachmentCorrespondencesList.head

    firstDocAttachmentCorrespondenceHub.corr_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceHub.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceHub.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceHub.doc_atchmnt_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceHub.corr_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.internalHandle.interface_identifier).get)

    Then("HubInvolvementDocAttachmentCorrespondenceInfos - check that the involvement doc attachments correspondences contain correct info")

    val sInvolvementDocAttachmentCorrespondenceInfosPartitionedPath = sInvolvementDocAttachCorrespondenceInfoPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvementDocAttachmentCorrespondenceInfosPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sInvolvementDocAttachmentCorrespondenceInfosFiles = sInvolvementDocAttachmentCorrespondenceInfosPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSInvolvementDocAttachmentCorrespondenceInfosList: List[SServiceDeliveryCorrInfo] = sInvolvementDocAttachmentCorrespondenceInfosFiles.flatMap(
      sInvolvementDocAttachmentCorrespondenceInfo => Source.fromFile(sInvolvementDocAttachmentCorrespondenceInfo).getLines().toList.map(sInvolvementDocAttachmentCorrespondenceInfoJson => parse(sInvolvementDocAttachmentCorrespondenceInfoJson).extract[SServiceDeliveryCorrInfo]))

    actualSInvolvementDocAttachmentCorrespondenceInfosList should have size expectedSD.involvements.flatMap(_.documentAttachments.flatMap(_.correspondences.map(_.size))).sum

    val firstDocAttachmentCorrespondenceInfoSatellite = actualSInvolvementDocAttachmentCorrespondenceInfosList.head

    firstDocAttachmentCorrespondenceInfoSatellite.corr_rec_hash_value should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.lnk_corr_addr_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.srvc_dlvry_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.invlmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.doc_atchmnt_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.corr_hk should not equal HashHelper.emptyHash
    firstDocAttachmentCorrespondenceInfoSatellite.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceInfoSatellite.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceInfoSatellite.doc_atchmnt_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.internalHandle.interface_identifier)
    firstDocAttachmentCorrespondenceInfoSatellite.corr_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.internalHandle.interface_identifier).get)
    firstDocAttachmentCorrespondenceInfoSatellite.corr_delivery_note should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.deliveryNote).get)
    firstDocAttachmentCorrespondenceInfoSatellite.corr_status_cd should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.status).get)
    firstDocAttachmentCorrespondenceInfoSatellite.po_addr_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.postalAddressId).get)
    firstDocAttachmentCorrespondenceInfoSatellite.el_addr_handle_id should equal(expectedSD.involvements.sorted.head.documentAttachments.sorted.head.correspondences.map(_.sorted.head.electronicAddressId).get)


    Then("SInvolvementPOAddresses - check that the involvement po addresses contain correct info")

    val sInvolvementPOAddressesPartitionedPath = sInvolvementPOAddressesPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvementPOAddressesPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sInvolvementPOAddressesFiles = sInvolvementPOAddressesPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSInvolvementPOAddressesList: List[SServiceDeliveryInvolvementPostalAddress] = sInvolvementPOAddressesFiles.flatMap(
      sInvolvementPOAddressInfo => Source.fromFile(sInvolvementPOAddressInfo).getLines().toList.map(sInvolvementPOAddressJson => parse(sInvolvementPOAddressJson).extract[SServiceDeliveryInvolvementPostalAddress]))

    actualSInvolvementPOAddressesList should have size expectedSD.involvements.flatMap(_.involvementAddresses.map(_.size)).sum + expectedSD.indirectInvolvements.flatMap(_.involvementAddresses.map(_.size)).sum

    val firstDirectPostalAddress = actualSInvolvementPOAddressesList.filter(_.invlmnt_method_cd.equals("DIRECT")).head
    firstDirectPostalAddress.po_addr_hk should not equal HashHelper.emptyHash
    firstDirectPostalAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstDirectPostalAddress.lnk_invlmnt_po_addr_hk should not equal HashHelper.emptyHash
    firstDirectPostalAddress.invlmnt_po_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstDirectPostalAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstDirectPostalAddress.invlmnt_po_addr_handle_id should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstDirectPostalAddress.invlmnt_po_addr_usage_type_cd should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.addressUsageType.map(_.code))
    firstDirectPostalAddress.po_addr_handle_id should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.map(_.interface_identifier))
    firstDirectPostalAddress.invlmnt_po_addr_visibility should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.flatMap(_.visibility_marker))
    firstDirectPostalAddress.invlmnt_po_addr_start_dtime should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstDirectPostalAddress.invlmnt_po_addr_end_dtime should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstDirectPostalAddress.invlmnt_po_addr_created_by should equal(expectedSD.involvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.createdBy)

    val firstIndirectPostalAddress = actualSInvolvementPOAddressesList.filter(_.invlmnt_method_cd.equals("INDIRECT")).head
    firstIndirectPostalAddress.po_addr_hk should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.lnk_invlmnt_po_addr_hk should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.invlmnt_po_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstIndirectPostalAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstIndirectPostalAddress.invlmnt_po_addr_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstIndirectPostalAddress.invlmnt_po_addr_usage_type_cd should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.addressUsageType.map(_.code))
    firstIndirectPostalAddress.po_addr_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.map(_.interface_identifier))
    firstIndirectPostalAddress.invlmnt_po_addr_visibility should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.postalAddressHandle.flatMap(_.visibility_marker))
    firstIndirectPostalAddress.invlmnt_po_addr_start_dtime should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstIndirectPostalAddress.invlmnt_po_addr_end_dtime should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstIndirectPostalAddress.invlmnt_po_addr_created_by should equal(expectedSD.indirectInvolvements.sorted.head.involvementAddresses.getOrElse(List()).sorted.head.createdBy)

    Then("SInvolvementElAddresses - check that the involvement electronic addresses contain correct info")

    val sInvolvementElAddressesPartitionedPath = sInvolvementElAddressesPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvementElAddressesPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sInvolvementElAddressesFiles = sInvolvementElAddressesPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actualSInvolvementElAddressesList: List[SServiceDeliveryInvolvementElectronicAddress] = sInvolvementElAddressesFiles.flatMap(
      sInvolvementElAddressInfo => Source.fromFile(sInvolvementElAddressInfo).getLines().toList.map(sInvolvementElAddressJson => parse(sInvolvementElAddressJson).extract[SServiceDeliveryInvolvementElectronicAddress]))

    actualSInvolvementElAddressesList should have size expectedSD.involvements.flatMap(_.electronicAddresses.map(_.size)).sum + expectedSD.indirectInvolvements.flatMap(_.electronicAddresses.map(_.size)).sum

    val firstElectronicAddress = actualSInvolvementElAddressesList.filter(_.invlmnt_method_cd.equals("DIRECT")).head
    firstElectronicAddress.el_addr_hk should not equal HashHelper.emptyHash
    firstElectronicAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstElectronicAddress.lnk_invlmnt_el_addr_hk should not equal HashHelper.emptyHash
    firstElectronicAddress.invlmnt_el_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstElectronicAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstElectronicAddress.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstElectronicAddress.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)

    firstElectronicAddress.invlmnt_el_addr_handle_id should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstElectronicAddress.invlmnt_el_addr_type_cd should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.`type`)
    firstElectronicAddress.invlmnt_el_addr_visibility should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.visibility_marker)
    firstElectronicAddress.invlmnt_el_addr_value should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.value)
    firstElectronicAddress.invlmnt_el_addr_start_dtime should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstElectronicAddress.invlmnt_el_addr_end_dtime should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstElectronicAddress.invlmnt_el_addr_created_by should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.createdBy)
    firstElectronicAddress.invlmnt_el_addr_created_dtime should equal(expectedSD.involvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.created)

    val firstIndirectElectronicAddress = actualSInvolvementElAddressesList.filter(_.invlmnt_method_cd.equals("INDIRECT")).head
    firstIndirectElectronicAddress.el_addr_hk should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.rec_hash_value should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.lnk_invlmnt_el_addr_hk should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.invlmnt_el_addr_list_agg_hash should not equal HashHelper.emptyHash
    firstIndirectElectronicAddress.invlmnt_hk should not equal HashHelper.emptyHash

    firstIndirectElectronicAddress.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstIndirectElectronicAddress.invlmnt_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.internalHandle.interface_identifier)

    firstIndirectElectronicAddress.invlmnt_el_addr_handle_id should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.interface_identifier)
    firstIndirectElectronicAddress.invlmnt_el_addr_type_cd should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.`type`)
    firstIndirectElectronicAddress.invlmnt_el_addr_visibility should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.internalHandle.visibility_marker)
    firstIndirectElectronicAddress.invlmnt_el_addr_value should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.value)
    firstIndirectElectronicAddress.invlmnt_el_addr_start_dtime should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveStartDate)
    firstIndirectElectronicAddress.invlmnt_el_addr_end_dtime should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.effectiveEndDate)
    firstIndirectElectronicAddress.invlmnt_el_addr_created_by should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.createdBy)
    firstIndirectElectronicAddress.invlmnt_el_addr_created_dtime should equal(expectedSD.indirectInvolvements.sorted.head.electronicAddresses.getOrElse(List()).sorted.head.created)

    Then("SServiceDeliveryInvolvementNote - check that the involvement notes contain correct values")

    val sInvolvementNotesPartitionedPath = sInvolvementNotesPath.resolve(f"year=${time.getYear}%04d/month=${time.getMonthOfYear}%02d/day=${time.getDayOfMonth}%02d/hour=${time.getHourOfDay}%02d/batch-1000")

    eventually(sInvolvementNotesPartitionedPath.toFile.list().toList should contain("_SUCCESS"))

    val sSDInvolvementNotesFiles = sInvolvementNotesPartitionedPath.toFile.listFiles(new PartitionedFilter()).toList

    val actuallSDInvolvementNotesList: List[SServiceDeliveryInvolvementNote] = sSDInvolvementNotesFiles.flatMap(
      sSDInvolvementNote => Source.fromFile(sSDInvolvementNote).getLines().toList.map(sSDInvolvementNoteJson => parse(sSDInvolvementNoteJson).extract[SServiceDeliveryInvolvementNote]))

    actuallSDInvolvementNotesList should have size expectedSD.involvements.map(_.notes.size).sum

    val firstInvolvementNote = actuallSDInvolvementNotesList.head
    firstInvolvementNote.srvc_dlvry_handle_id should equal(expectedSD.internalHandle.interface_identifier)
    firstInvolvementNote.invlmnt_handle_id should equal(expectedSD.involvements.sorted.head.internalHandle.interface_identifier)

    firstInvolvementNote.rec_hash_value should not equal HashHelper.emptyHash
    firstInvolvementNote.invlmnt_hk should not equal HashHelper.emptyHash
    firstInvolvementNote.note_id should equal(expectedSD.involvements.sorted.head.notes.sorted.head.id.toString())
    firstInvolvementNote.note_type_cd should equal(expectedSD.involvements.sorted.head.notes.sorted.head.`type`.refDataValueCode)
    firstInvolvementNote.note_text should equal(expectedSD.involvements.sorted.head.notes.sorted.head.noteText)

    deleteFile(hubInvolvementPath.toFile)
    deleteFile(sInvolvementInfoPath.toFile)
    deleteFile(hubInvolvedPersonPath.toFile)
    deleteFile(sInvolvedPersonInfoPath.toFile)
    deleteFile(sInvolvedPersonRecentBiographicsPath.toFile)
    deleteFile(lInvolvementPartyPath.toFile)
    deleteFile(hubInvolvementDocAttachmentsPath.toFile)
    deleteFile(sDocAttachmentInfoPath.toFile)
    deleteFile(hubInvolvementDocAttachCorrespondencePath.toFile)
    deleteFile(sInvolvementDocAttachCorrespondenceInfoPath.toFile)
    deleteFile(sInvolvementPOAddressesPath.toFile)
    deleteFile(sInvolvementElAddressesPath.toFile)
    deleteFile(sInvolvementNotesPath.toFile)

  }

}
