package uk.gov..mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.model.{Identity, IdentityMedia, MediaSet, SIdentityMedia}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper._


object IdentityMediaTransformer {

  def identityMedias(mediaSets: Option[Set[MediaSet]], messageId: String, identity: Identity, timestamp: Long): List[SIdentityMedia] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val media_super_set_agg_hash = superSetHash(mediaSets)

    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    mediaSets.getOrElse(Set()).toList.sorted.flatMap { case mediaSet: MediaSet =>
      val lnk_idntty_media_set_hk = linkMediaSetHK(source, identity.internal_handle.interface_identifier, mediaSet.internalHandle.interface_identifier)
      val media_set_agg_list = mediasAggStr(mediaSet)
      val media_set_agg_hash = sha1(media_set_agg_list)
      val media_set_rec_hash_value = sha1(mediaSetStr(mediaSet) ++ media_set_agg_list)
      if (mediaSet.identityMedias.isDefined) {
        mediaSet.identityMedias.get.toList.sorted.zipWithIndex.map { case (media: IdentityMedia, i: Int) =>
          val media_rec_has_value = sha1(mediaStr(media))
          SIdentityMedia(messageId, identity.created, fmt.print(time),
            source, identity.internal_handle.interface_identifier, media_super_set_agg_hash,
            mediaSet.internalHandle.interface_identifier, media_set_rec_hash_value, mediaSet.internalHandle.visibility_marker,
            mediaSet.externalHandle.flatMap(_.handle_value), mediaSet.externalHandle.flatMap(_.handle_space), mediaSet.createdBy,
            mediaSet.created, media_set_agg_hash, lnk_idntty_media_set_hk,
            Some(media.internalHandle.interface_identifier), Some(i), Some(media_rec_has_value),
            media.internalHandle.visibility_marker, media.identityMediaType, media.fileHandle.map(_.interface_identifier),
            media.createdBy, media.created, identity_hk)
        }
      } else {
        List(SIdentityMedia(messageId, identity.created, fmt.print(time),
          source, identity.internal_handle.interface_identifier, media_super_set_agg_hash,
          mediaSet.internalHandle.interface_identifier, media_set_rec_hash_value, mediaSet.internalHandle.visibility_marker,
          mediaSet.externalHandle.flatMap(_.handle_value), mediaSet.externalHandle.flatMap(_.handle_space), mediaSet.createdBy,
          mediaSet.created, media_set_agg_hash, lnk_idntty_media_set_hk,
          None, None, None,
          None, None, None,
          None, None, identity_hk))
      }
    }
  }

  def superSetHash(mediaSets: Option[Set[MediaSet]]): String = {
    val media_superset_agg_str: List[(String, String)] = mediaSets.map { case mediaSets: Set[MediaSet] =>
      mediaSets.toList.sorted.flatMap(mediaSet => {
        (mediaSetStr(mediaSet) ++ mediasAggStr(mediaSet))
      })
    }.getOrElse(List())
    sha1(media_superset_agg_str)
  }

  def mediaSetStr(mediaSet: MediaSet): List[(String, String)] = List(
    ("media_set_handle_visibility", mediaSet.internalHandle.visibility_marker.getOrElse("")),
    ("media_set_ext_handle_value", mediaSet.externalHandle.flatMap(_.handle_value).getOrElse("")),
    ("media_set_ext_handle_space", mediaSet.externalHandle.flatMap(_.handle_space).getOrElse("")),
    ("media_set_created_by", mediaSet.createdBy.getOrElse(""))
  )

  def mediasAggStr(mediaSet: MediaSet): List[(String, String)] = {
    mediaSet.identityMedias.getOrElse(Set()).toList.sorted.flatMap(mediaStr(_))
  }

  def mediaStr(media: IdentityMedia): List[(String, String)] = List(
    ("media_handle_visibility", media.internalHandle.visibility_marker.getOrElse("")),
    ("media_type_cd", media.identityMediaType.getOrElse("")),
    ("media_file_handle_id", media.fileHandle.map(_.interface_identifier).getOrElse("")),
    ("media_created_by", media.createdBy.getOrElse(""))
  )

  def linkMediaSetHK(source: String, ih_id: String, media_set_ih_id: String): String = sha1(Seq(("record_source", source), ("identity_handle_id", ih_id), ("media_set_handle_id", media_set_ih_id)))

}
