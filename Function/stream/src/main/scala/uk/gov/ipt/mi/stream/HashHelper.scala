package uk.gov.ipt.mi.stream

import org.apache.commons.codec.digest.DigestUtils

object HashHelper {
  def sha1(input: String): String = DigestUtils.shaHex(input)

  def sha1(tuples: Seq[(String, String)]): String = sha1(tuples.map { case (k: String, v: String) => k + ':' + v }.mkString("|"))

  def emptyHash = sha1(Seq())
}
