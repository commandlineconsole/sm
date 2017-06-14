package uk.gov.ipt.mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.model.{Biographic, BiographicSet, Identity, SIdentityBiographics}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper.sha1


object IdentityBiographicTransformer {

  def identityBiographics(biographicSets: Option[Set[BiographicSet]], messageId: String, identity: Identity, timestamp: Long): List[SIdentityBiographics] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)
    val biog_superset_agg_hash = superSetHash(biographicSets)
    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    biographicSets.getOrElse(Set()).toList.sorted.flatMap { case (biographicSet: BiographicSet) =>
      val lnk_idntty_biog_set_hk = linkIdentityBiogSet(source, identity.internal_handle.interface_identifier, biographicSet.internal_handle.interface_identifier)
      val biog_set_agg_string =  biographicsAggStr(biographicSet)
      val biog_set_agg_hash =  sha1(biog_set_agg_string)
      val biog_set_rec_hash_value = sha1(biographicSetStr(biographicSet) ++ biog_set_agg_string)

      val biographics = biographicSet.biographics.toList.sortBy(_._1).zipWithIndex

      if (biographics.isEmpty){
        List(SIdentityBiographics(messageId, identity.created, fmt.print(time),
          source, identity.internal_handle.interface_identifier, biog_superset_agg_hash,
          biographicSet.internal_handle.interface_identifier, biog_set_rec_hash_value, biographicSet.internal_handle.visibility_marker,
          biographicSet.external_handle.flatMap(_.handle_value), biographicSet.external_handle.flatMap(_.handle_space), biographicSet.biographic_set_purpose.flatMap(_.code),
          biographicSet.created_by, biographicSet.created, biog_set_agg_hash,
          lnk_idntty_biog_set_hk, None, None, None,
          None, None, None,
          None, None, None,
          None, None, identity_hk
        ))
      }else {
        biographics.map { case((biographic_key:String, biographic:Biographic), i:Int) =>
          val biographic_rec_hash_value = sha1(biographicStr(biographic_key, biographic))
          SIdentityBiographics(messageId, identity.created, fmt.print(time),
            source, identity.internal_handle.interface_identifier, biog_superset_agg_hash,
            biographicSet.internal_handle.interface_identifier, biog_set_rec_hash_value, biographicSet.internal_handle.visibility_marker,
            biographicSet.external_handle.flatMap(_.handle_value), biographicSet.external_handle.flatMap(_.handle_space), biographicSet.biographic_set_purpose.flatMap(_.code),
            biographicSet.created_by, biographicSet.created, biog_set_agg_hash,
            lnk_idntty_biog_set_hk, Some(biographic_key), Some(biographic.internal_handle.interface_identifier), Some(i),
            Some(biographic_rec_hash_value), biographic.internal_handle.visibility_marker, biographic.`type`,
            biographic.value, biographic.value_type, biographic.created_by,
            biographic.created, biographic.reference_data_set, identity_hk
          )
        }
      }
    }
  }

  def superSetHash(biographicSets: Option[Set[BiographicSet]]): String = {
    val biog_super_set_agg_str = biographicSets.map { case biographicSets: Set[BiographicSet] =>
      biographicSets.toList.sorted.flatMap(biographicSet => {
        (biographicSetStr(biographicSet) ++ biographicsAggStr(biographicSet))
      })

    }.getOrElse(List())
    HashHelper.sha1(biog_super_set_agg_str)
  }

  def biographicSetStr(biographicSet: BiographicSet): List[(String, String)] = {
    List(
      ("biog_set_handle_visibility", biographicSet.internal_handle.visibility_marker.getOrElse("")),
      ("biog_set_ext_handle_value", biographicSet.external_handle.flatMap(_.handle_value).getOrElse("")),
      ("biog_set_ext_handle_space", biographicSet.external_handle.flatMap(_.handle_space).getOrElse("")),
      ("biog_set_purpose_cd", biographicSet.biographic_set_purpose.flatMap(_.code).getOrElse("")),
      ("biog_set_created_by", biographicSet.created_by.getOrElse(""))
    )
  }

  def biographicsAggStr(biographicSet: BiographicSet): List[(String, String)] = {
    biographicSet.biographics.toList.sortBy(_._1).flatMap { case (biographic_key: String, biographic: Biographic) =>
      biographicStr(biographic_key, biographic)
    }
  }

  def biographicStr(biographic_key: String, biographic: Biographic): List[(String, String)] =
    List(
      ("biographic_key", biographic_key),
      ("biographic_handle_visibility", biographic.internal_handle.visibility_marker.getOrElse("")),
      ("biographic_type_cd", biographic.`type`.getOrElse("")),
      ("biographic_value", biographic.value.getOrElse("")),
      ("biographic_value_type", biographic.value_type.getOrElse("")),
      ("biographic_created_by", biographic.created_by.getOrElse("")),
      ("biographic_reference_data_set", biographic.reference_data_set.getOrElse("")))

  def linkIdentityBiogSet(source: String, ih_id: String, biog_set_ih_id: String) = sha1(Seq(("record_source", source), ("identity_handle_id", ih_id), ("biog_set_handle_id", biog_set_ih_id)))
}
