package uk.gov.ipt.mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov.ipt.mi.model.{Identity, IdentityCondition, SIdentityCondition}
import uk.gov.ipt.mi.stream.HashHelper
import uk.gov.ipt.mi.stream.HashHelper.sha1


object IdentityConditionTransformer {

  def identityCondition(messageId: String, identity: Identity, timestamp: Long): List[SIdentityCondition] = {
    val source = "IPT"
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)

    val collectionStr = conditionAggStr(identity)

    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    val condition_list_agg_hash = sha1(collectionStr)
    val lnk_idntty_condition_list_hk = sha1(Seq(("record_source", source), ("identity_handle_id", identity.internal_handle.interface_identifier)) ++ collectionStr)

    identity.conditions.getOrElse(Set()).toList.sorted.zipWithIndex.map { case (condition: IdentityCondition, i: Int) =>
      val rec_hash_value = sha1(conditionStr(condition))

      SIdentityCondition(messageId, identity.created, fmt.print(time),
        source, identity.internal_handle.interface_identifier, condition.internalHandle.interface_identifier,
        i, rec_hash_value, condition.internalHandle.visibility_marker,
        condition.externalHandle.flatMap(_.handle_value), condition.externalHandle.flatMap(_.handle_space), condition.conditionDate,
        condition.conditionEndDate, condition.conditionType.flatMap(_.code), condition.conditionUpdateDate,
        condition.conditionNote, condition.createdBy, condition.created,
        condition.originatingUser, condition_list_agg_hash, lnk_idntty_condition_list_hk,
        identity_hk
      )
    }
  }

  def conditionAggStr(identity: Identity): List[(String, String)] = {
    identity.conditions.map { case conditions: Set[IdentityCondition] =>
      conditions.toList.sorted.flatMap(conditionStr(_))
    }.getOrElse(List())
  }

  def conditionStr(condition: IdentityCondition): List[(String, String)] = {
    List(
      ("condition_handle_visibility", condition.internalHandle.visibility_marker.getOrElse("")),
      ("condition_ext_handle_value", condition.externalHandle.flatMap(_.handle_value).getOrElse("")),
      ("condition_ext_handle_space", condition.externalHandle.flatMap(_.handle_space).getOrElse("")),
      ("condition_type_cd", condition.conditionType.flatMap(_.code).getOrElse("")),
      ("condition_note", condition.conditionNote.getOrElse("")),
      ("condition_created_by", condition.createdBy.getOrElse("")),
      ("condition_originated_by", condition.originatingUser.getOrElse(""))
    )
  }
}


