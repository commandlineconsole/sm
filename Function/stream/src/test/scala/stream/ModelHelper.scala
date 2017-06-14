package uk.gov..mi.stream

import uk.gov..mi.model.{ExternalHandle, InternalHandle}

object ModelHelper {

  def internalHandle(id: String) = InternalHandle(id, s"$id-uniqueId", Some("DEFAULT"))

  def externalHandle(id: String) = ExternalHandle(id, Some(s"$id-value"), Some(""))

}
