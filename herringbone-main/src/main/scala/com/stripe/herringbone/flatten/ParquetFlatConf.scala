package com.stripe.herringbone.flatten

import org.rogach.scallop._

class ParquetFlatConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val inputPath = opt[String](required = true)
  val outputPath = opt[String](descr = "Default is input path with `-flat` or `-tsv` appended as appropriate")
  val previousPath = opt[String](descr = "Path of previously generated flat output, so field ordering can be maintained (optional)")
  val separator = opt[String](default = Some("__"))
  val renameId = opt[Boolean](descr = "Flatten a.b.id as a__b instead of a__b__id")
}
