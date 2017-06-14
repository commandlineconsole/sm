package uk.gov..mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.model.{Descrion, Descror, Identity, SIdentityDescrors}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper.sha1

object IdentityDescrionTransformer {

  def identityDescrors(descrions: Option[Set[Descrion]], messageId: String, identity: Identity, timestamp: Long): List[SIdentityDescrors] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val descr_super_set_agg_hash = superSetHash(descrions)
    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    descrions.getOrElse(Set()).toList.sorted.flatMap { case (descrion: Descrion) =>
      val lnk_idntty_descr_set_hk = linkIdentityDescrionSetHK(source, identity.internal_handle.interface_identifier, descrion.internalHandle.interface_identifier)
      val descr_set_agg_list = descrorsAggStr(descrion)
      val descr_set_agg_hash = sha1(descr_set_agg_list)
      val descr_set_rec_hash_value = sha1(descrionSetStr(descrion) ++ descr_set_agg_list)

      if (descrion.descrors.isDefined) {
        descrion.descrors.get.toList.sorted.zipWithIndex.map { case (descror: Descror, i: Int) =>
          val descr_rec_hash_value = sha1(descrorStr(descror))

          SIdentityDescrors(messageId, identity.created, fmt.print(time),
            source, identity.internal_handle.interface_identifier, descr_super_set_agg_hash,
            descrion.internalHandle.interface_identifier, descr_set_rec_hash_value, descrion.internalHandle.visibility_marker,
            descrion.externalHandle.flatMap(_.handle_value), descrion.externalHandle.flatMap(_.handle_space), descrion.createdBy,
            descrion.created, descr_set_agg_hash, lnk_idntty_descr_set_hk,
            Some(descror.internalHandle.interface_identifier), Some(i), Some(descr_rec_hash_value),
            descror.internalHandle.visibility_marker, descror.descrorType, descror.descrorValue,
            descror.createdBy, descror.created, identity_hk
          )
        }
      } else {
        List(SIdentityDescrors(messageId, identity.created, fmt.print(time),
          source, identity.internal_handle.interface_identifier, descr_super_set_agg_hash,
          descrion.internalHandle.interface_identifier, descr_set_rec_hash_value, descrion.internalHandle.visibility_marker,
          descrion.externalHandle.flatMap(_.handle_value), descrion.externalHandle.flatMap(_.handle_space), descrion.createdBy,
          descrion.created, descr_set_agg_hash, lnk_idntty_descr_set_hk,
          None, None, None,
          None, None, None,
          None, None, identity_hk
        ))
      }
    }
  }

  def superSetHash(descrions: Option[Set[Descrion]]): String = {
    val descr_superset_agg_str = descrions.map { case descrions: Set[Descrion] =>
      descrions.toList.sorted.flatMap(descrion => {
        (descrionSetStr(descrion) ++ descrorsAggStr(descrion))
      })
    }.getOrElse(List())
    HashHelper.sha1(descr_superset_agg_str)
  }

  def linkIdentityDescrionSetHK(source: String, ih_id: String, descr_set_ih_id: String): String = {
    sha1(Seq(("record_source", source), ("identity_handle_id", ih_id), ("descr_set_handle_id", descr_set_ih_id)))
  }

  def descrorsAggStr(descrion: Descrion): List[(String, String)] = {
    descrion.descrors.getOrElse(Set()).toList.sorted.flatMap(descrorStr(_))
  }

  def descrionSetStr(descrion: Descrion): List[(String, String)] = {
    List(
      ("descr_set_handle_visibility", descrion.internalHandle.visibility_marker.getOrElse("")),
      ("descr_set_ext_handle_value", descrion.externalHandle.flatMap(_.handle_value).getOrElse("")),
      ("descr_set_ext_handle_space", descrion.externalHandle.flatMap(_.handle_space).getOrElse("")),
      ("descr_set_created_by", descrion.createdBy.getOrElse("")))
  }

  def descrorStr(descror: Descror): List[(String, String)] = {
    List(
      ("descr_handle_visibility", descror.internalHandle.visibility_marker.getOrElse("")),
      ("descr_type_cd", descror.descrorType.getOrElse("")),
      ("descr_value", descror.descrorValue.getOrElse("")),
      ("descr_created_by", descror.createdBy.getOrElse(""))
    )
  }

}
