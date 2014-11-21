package com.stripe.herringbone.load

import org.rogach.scallop._

class ParquetLoadConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val database = opt[String](default = Some("default"))
  val table = opt[String](required = true)
  val path = opt[String]()
  val hive = opt[Boolean]("hive")
  val connectionUrl = opt[String](required = true)
  val connectionPort = opt[String](required = true)

  val updatePartitions = toggle(descrYes = "Create table if not present, otherwise update with new partitions", default = Some(false))
  validateOpt (path, updatePartitions) {
    case (None, None) => Left("You must specify at least one of path or update-partitions")
    case _ => Right(Unit)
  }
}
