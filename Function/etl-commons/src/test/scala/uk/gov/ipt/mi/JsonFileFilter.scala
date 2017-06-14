package uk.gov..mi

import java.io.{File, FilenameFilter}

class JsonFileFilter extends FilenameFilter {
  override def accept(dir: File, name: String): Boolean = name.endsWith(".json")
}
class PartitionedFilter extends FilenameFilter {
  override def accept(dir: File, name: String): Boolean = name.startsWith("part-")
}
