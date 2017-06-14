package uk.gov..mi.stream.servicedelivery

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.DateHelper
import uk.gov..mi.model.SServiceDeliveryInvolvedPersonInfo
import uk.gov..mi.model.servicedelivery.ServiceDelivery
import uk.gov..mi.stream.HashHelper


object SInvolvedPersonInfoTransformer {
  def involvedPersonInfo(messageId: String, serviceDelivery: ServiceDelivery, timestamp: Long): List[SServiceDeliveryInvolvedPersonInfo] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    serviceDelivery.involvements.filter(_.person.isDefined).sorted.map(involvement => {

      val title_cd = involvement.person.flatMap(_.defaultBiographics.map(_.title.code)).getOrElse("")
      val lang_cd = involvement.person.flatMap(_.defaultBiographics.map(_.language.code)).getOrElse("")
      val given_name = involvement.person.flatMap(_.defaultBiographics.map(_.givenName)).getOrElse("")
      val family_name = involvement.person.flatMap(_.defaultBiographics.map(_.familyName)).getOrElse("")
      val full_name = involvement.person.flatMap(_.defaultBiographics.map(_.fullName)).getOrElse("")
      val gender_code = involvement.person.flatMap(_.defaultBiographics.map(_.gender.code)).getOrElse("")
      val day_of_birth = involvement.person.flatMap(_.defaultBiographics.map(_.dayOfBirth.toString)).getOrElse("")
      val month_of_birth = involvement.person.flatMap(_.defaultBiographics.map(_.monthOfBirth.toString)).getOrElse("")
      val year_of_birth = involvement.person.flatMap(_.defaultBiographics.map(_.yearOfBirth.toString)).getOrElse("")
      val date_of_birth = involvement.person.flatMap(_.defaultBiographics.map(_.dateOfBirth)).getOrElse("")
      val photo_url = involvement.person.flatMap(_.defaultBiographics.map(_.photoUrl)).getOrElse("")
      val nationality_cd = involvement.person.flatMap(_.defaultBiographics.map(_.nationality.code)).getOrElse("")
      val place_of_birth = involvement.person.flatMap(_.defaultBiographics.map(_.placeOfBirth)).getOrElse("")
      val passport = involvement.person.flatMap(_.defaultBiographics.map(_.passport)).getOrElse("")

      val chlg_rec_hash_value = HashHelper.sha1(Seq(
        ("messageId",messageId),
        ("src_created_by", involvement.createdUserId.getOrElse("")),
        ("last_updated_by", involvement.lastUpdatedUserId.getOrElse(""))
      ))

      val involved_person_hk = HashHelper.sha1(Seq(
        ("record_source", source),
        ("srvc_dlvry_handle_id", serviceDelivery.internalHandle.interface_identifier),
        ("involved_person_id", involvement.person.map(_.personId).getOrElse(""))
      ))

      val default_biog_rec_hash_value = HashHelper.sha1(Seq(
        ("title_cd", title_cd),
        ("lang_cd", lang_cd),
        ("given_name", given_name),
        ("family_name", family_name),
        ("full_name", full_name),
        ("gender_cd", gender_code),
        ("day_of_birth", day_of_birth),
        ("month_of_birth", month_of_birth),
        ("year_of_birth", year_of_birth),
        ("date_of_birth", date_of_birth),
        ("photo_url", photo_url),
        ("nationality_cd", nationality_cd),
        ("place_of_birth", place_of_birth),
        ("place_of_birth", passport)
      ))

      SServiceDeliveryInvolvedPersonInfo(messageId, DateHelper.getMostRecentDate(List(serviceDelivery.created, serviceDelivery.createdDate, serviceDelivery.lastUpdated)), fmt.print(time),
                                         source, serviceDelivery.internalHandle.interface_identifier, involvement.person.map(_.personId),
                                         chlg_rec_hash_value, involvement.person.flatMap(_.createdUserId), involvement.person.flatMap(_.createdDate),
                                         default_biog_rec_hash_value, title_cd, lang_cd,
                                         given_name, family_name, full_name,
                                         gender_code, day_of_birth, month_of_birth,
                                         year_of_birth, date_of_birth, photo_url,
                                         nationality_cd, place_of_birth, passport,
                                         involved_person_hk
      )
    })

  }

}
