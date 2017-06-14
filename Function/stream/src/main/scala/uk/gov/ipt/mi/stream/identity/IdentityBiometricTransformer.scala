package uk.gov..mi.stream.identity

import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import uk.gov..mi.model.{Biometric, Identity, SIdentityBiometricInfo}
import uk.gov..mi.stream.HashHelper
import uk.gov..mi.stream.HashHelper.sha1


object IdentityBiometricTransformer {

  def identityBiometric(messageId: String, identity: Identity, timestamp: Long): List[SIdentityBiometricInfo] = {
    val source = ""
    val fmt = ISODateTimeFormat.dateTime()
    val time = new DateTime(timestamp, DateTimeZone.UTC)
    val collectionStr = biometricSetAggStr(identity)

    val identity_hk = HashHelper.sha1(Seq(
      ("record_source", source),
      ("identity_handle_id", identity.internal_handle.interface_identifier))
    )
    val biometric_list_agg_hash = sha1(collectionStr)

    val lnk_idntty_biometric_list_hk = sha1(Seq(("record_source", source), ("identity_handle_id", identity.internal_handle.interface_identifier)) ++ collectionStr)

    identity.biometrics.getOrElse(Set()).toList.sorted.zipWithIndex.map {
      case (biometric: Biometric, i: Int) => {
        val rec_hash_value = sha1(biometricStr(biometric))


        SIdentityBiometricInfo(messageId, identity.created, fmt.print(time),
          source, identity.internal_handle.interface_identifier, biometric.internalHandle.interface_identifier,
          i, rec_hash_value, biometric.internalHandle.visibility_marker,
          biometric.externalHandle.flatMap(_.handle_value), biometric.externalHandle.flatMap(_.handle_space), biometric.biometricNature,
          biometric.biometricReferenceType, biometric.biometricReferenceValue, biometric.created_by,
          biometric.created, biometric_list_agg_hash, lnk_idntty_biometric_list_hk,
          identity_hk
        )
      }
    }
  }

  def biometricSetAggStr(identity: Identity) = {
    identity.biometrics
      .map {
        case biometrics: Set[Biometric] =>
          biometrics.toList.sorted
            .flatMap(biometricStr(_))
      }
      .getOrElse(List())
  }

  def biometricStr(biometric: Biometric): List[(String, String)] = {
    List(
      ("biometric_handle_visibility", biometric.internalHandle.visibility_marker.getOrElse("")),
      ("biometric_ext_handle_value", biometric.externalHandle.flatMap(_.handle_value).getOrElse("")),
      ("biometric_ext_handle_space", biometric.externalHandle.flatMap(_.handle_space).getOrElse("")),
      ("biometric_nature", biometric.biometricNature.getOrElse("")),
      ("biometric_type_cd", biometric.biometricReferenceType.getOrElse("")),
      ("biometric_value", biometric.biometricReferenceValue.getOrElse("")),
      ("biometric_created_by", biometric.created_by.getOrElse("")))
  }
}
