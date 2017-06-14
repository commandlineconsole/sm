package uk.gov.ipt.mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.model.{Description, Descriptor, Identity, SIdentityDescriptors}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper.sha1

object IdentityDescriptionTransformer {

  def identityDescriptors(descriptions: Option[Set[Description]], messageId: String, identity: Identity, timestamp: Long): List[SIdentityDescriptors] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val descr_super_set_agg_hash = superSetHash(descriptions)
    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    descriptions.getOrElse(Set()).toList.sorted.flatMap { case (description: Description) =>
      val lnk_idntty_descr_set_hk = linkIdentityDescriptionSetHK(source, identity.internal_handle.interface_identifier, description.internalHandle.interface_identifier)
      val descr_set_agg_list = descriptorsAggStr(description)
      val descr_set_agg_hash = sha1(descr_set_agg_list)
      val descr_set_rec_hash_value = sha1(descriptionSetStr(description) ++ descr_set_agg_list)

      if (description.descriptors.isDefined) {
        description.descriptors.get.toList.sorted.zipWithIndex.map { case (descriptor: Descriptor, i: Int) =>
          val descr_rec_hash_value = sha1(descriptorStr(descriptor))

          SIdentityDescriptors(messageId, identity.created, fmt.print(time),
            source, identity.internal_handle.interface_identifier, descr_super_set_agg_hash,
            description.internalHandle.interface_identifier, descr_set_rec_hash_value, description.internalHandle.visibility_marker,
            description.externalHandle.flatMap(_.handle_value), description.externalHandle.flatMap(_.handle_space), description.createdBy,
            description.created, descr_set_agg_hash, lnk_idntty_descr_set_hk,
            Some(descriptor.internalHandle.interface_identifier), Some(i), Some(descr_rec_hash_value),
            descriptor.internalHandle.visibility_marker, descriptor.descriptorType, descriptor.descriptorValue,
            descriptor.createdBy, descriptor.created, identity_hk
          )
        }
      } else {
        List(SIdentityDescriptors(messageId, identity.created, fmt.print(time),
          source, identity.internal_handle.interface_identifier, descr_super_set_agg_hash,
          description.internalHandle.interface_identifier, descr_set_rec_hash_value, description.internalHandle.visibility_marker,
          description.externalHandle.flatMap(_.handle_value), description.externalHandle.flatMap(_.handle_space), description.createdBy,
          description.created, descr_set_agg_hash, lnk_idntty_descr_set_hk,
          None, None, None,
          None, None, None,
          None, None, identity_hk
        ))
      }
    }
  }

  def superSetHash(descriptions: Option[Set[Description]]): String = {
    val descr_superset_agg_str = descriptions.map { case descriptions: Set[Description] =>
      descriptions.toList.sorted.flatMap(description => {
        (descriptionSetStr(description) ++ descriptorsAggStr(description))
      })
    }.getOrElse(List())
    HashHelper.sha1(descr_superset_agg_str)
  }

  def linkIdentityDescriptionSetHK(source: String, ih_id: String, descr_set_ih_id: String): String = {
    sha1(Seq(("record_source", source), ("identity_handle_id", ih_id), ("descr_set_handle_id", descr_set_ih_id)))
  }

  def descriptorsAggStr(description: Description): List[(String, String)] = {
    description.descriptors.getOrElse(Set()).toList.sorted.flatMap(descriptorStr(_))
  }

  def descriptionSetStr(description: Description): List[(String, String)] = {
    List(
      ("descr_set_handle_visibility", description.internalHandle.visibility_marker.getOrElse("")),
      ("descr_set_ext_handle_value", description.externalHandle.flatMap(_.handle_value).getOrElse("")),
      ("descr_set_ext_handle_space", description.externalHandle.flatMap(_.handle_space).getOrElse("")),
      ("descr_set_created_by", description.createdBy.getOrElse("")))
  }

  def descriptorStr(descriptor: Descriptor): List[(String, String)] = {
    List(
      ("descr_handle_visibility", descriptor.internalHandle.visibility_marker.getOrElse("")),
      ("descr_type_cd", descriptor.descriptorType.getOrElse("")),
      ("descr_value", descriptor.descriptorValue.getOrElse("")),
      ("descr_created_by", descriptor.createdBy.getOrElse(""))
    )
  }

}
