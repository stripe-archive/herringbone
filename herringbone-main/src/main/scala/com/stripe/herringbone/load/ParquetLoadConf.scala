package com.stripe.herringbone.load

import org.rogach.scallop._

class ParquetLoadConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val database = opt[String](default = Some("default"))
  val table = opt[String](required = true)
  val path = opt[String]()
  val hive = opt[Boolean]("hive")
  val connectionUrl = opt[String](required = true)
  val connectionPort = opt[String](required = true)
  val computeStats = toggle(descrYes = "Compute table stats after loading files into impala. Turn this off for faster loading into impala (but probably slower querying later on!)", default = Some(true))
  val updatePartitions = toggle(descrYes = "Create table if not present, otherwise update with new partitions. If a schema change is being made to an existing table, turn this off.", default = Some(false))
  validateOpt (path, updatePartitions) {
    case (None, None) => Left("You must specify at least one of path or update-partitions")
    case _ => Right(Unit)
  }
}
