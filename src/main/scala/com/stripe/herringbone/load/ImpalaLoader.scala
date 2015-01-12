package com.stripe.herringbone.load

import com.stripe.herringbone.impala.{ImpalaClient,ImpalaValue}

import org.apache.hadoop.conf._
import org.apache.hadoop.util._
import org.apache.hadoop.fs._

case class ImpalaLoader(conf: ParquetLoadConf,
  hadoopFs: HadoopFs,
  fieldUtils: FieldUtils) extends ParquetLoader {

  lazy val impalaClient = ImpalaClient(conf.connectionUrl(),
    conf.connectionPort().toInt)

  def checkTableExists(table: String, database: String): Boolean = {
    execute("USE %s".format(database))
    var exists: Boolean = false
    query("SHOW TABLES"){row =>
      row.foreach { value =>
        if (value.raw == table) exists = true
      }
    }
    exists
  }

  def createTable(pathString: String, table: String, database: String = "default") {
    val path = new Path(pathString)
    val location = hadoopFs.findAbsolutePath(path)
    val leafPaths = hadoopFs.findSortedLeafPaths(path)

    if(leafPaths.isEmpty)
      error("Could not find parquet files under " + path)

    val tableFields = fieldUtils.findTableFields(leafPaths.last)
    val partitionFields = fieldUtils.findPartitionFields(leafPaths.last)

    execute("CREATE DATABASE IF NOT EXISTS importing")
    execute("USE importing")

    createTableWithPartitionFields(location, table, tableFields, partitionFields)

    if(partitionFields.size > 0)
      addPartitions(table, leafPaths.map{hadoopFs.findPartitions(_)})

    execute("CREATE DATABASE IF NOT EXISTS %s".format(database))
    execute("DROP TABLE IF EXISTS %s.%s".format(database, table))
    execute("ALTER TABLE importing.%s RENAME TO %s.%s".format(table, database, table))
    if (partitionFields.isEmpty && conf.computeStats()) execute("COMPUTE STATS %s.%s".format(database, table))
  }

  def updateTable(table: String, database: String) {
    execute("USE %s".format(database))

    val basePath = findBasePath(table)
    val tablePartitions = findTablePartitions(table)
    val leafPaths = hadoopFs.findSortedLeafPaths(new Path(basePath))
    leafPaths.reverse.foreach{path =>
      val partitions = hadoopFs.findPartitions(path)
      if(!tablePartitions.contains(partitions.map{_._2}))
        addPartition(table, partitions)
    }
  }

  def findBasePath(table: String) = {
    var location: String = null
    query("DESCRIBE FORMATTED %s".format(table)){row =>
      if(row(0).raw.startsWith("Location:"))
        location = row(1).raw
    }
    location
  }

  def findTablePartitions(table: String) = {
    var partitions: List[List[String]] = Nil
    query("SHOW TABLE STATS %s".format(table)){row =>
      if(row.size > 4)
        partitions ::= List(row(0).raw)
    }
    partitions
  }

  def createTableWithPartitionFields(location: String, table: String, tableFields: List[String], partitionFields: List[String]) {
    execute("DROP TABLE IF EXISTS `%s`".format (table))

    val tableClause = "CREATE EXTERNAL TABLE IF NOT EXISTS `%s` (%s)".format(table, tableFields.mkString(", "))
    val partitionClause =
      if(partitionFields.isEmpty)
        ""
      else
        " PARTITIONED BY (%s)".format(partitionFields.mkString(" ,"))
    val storedClause = " STORED AS PARQUETFILE LOCATION \"%s\"".format(location)

    execute(tableClause + partitionClause + storedClause)
  }

  def addPartitions(table: String, partitions: List[List[(String, String)]]) {
    partitions.foreach{addPartition(table, _)}
  }

  def addPartition(table: String, partitions: List[(String,String)]) {
    val partitionClause =
      partitions.map {
        case (name, value) if(value.forall{_.isDigit}) =>
          "`%s`=%s".format(name, value)
         case (name, value) =>
          "`%s`='%s'".format(name, value)
        }.mkString(", ")

    execute("ALTER TABLE %s ADD IF NOT EXISTS PARTITION (%s)".format(table, partitionClause))
  }

  private def execute(stmt: String) {
    impalaClient.execute(stmt)
  }

  private def query(stmt: String)(fn: Seq[ImpalaValue] => Unit) {
    impalaClient.query(stmt){ r =>  fn(r) }
  }

  def closeConnection() = {}
}
