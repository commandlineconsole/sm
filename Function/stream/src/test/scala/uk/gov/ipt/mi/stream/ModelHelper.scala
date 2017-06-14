package uk.gov.ipt.mi.stream

import uk.gov.ipt.mi.model.{ExternalHandle, InternalHandle}

object ModelHelper {

  def internalHandle(id: String) = InternalHandle(id, s"$id-uniqueId", Some("DEFAULT"))

  def externalHandle(id: String) = ExternalHandle(id, Some(s"$id-value"), Some("IPT"))

}
