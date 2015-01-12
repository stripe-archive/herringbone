package com.stripe.herringbone

import com.stripe.herringbone.load._

import java.sql.ResultSet

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._

case class HiveLoader(conf: ParquetLoadConf,
  hadoopFs: HadoopFs,
  fieldUtils: FieldUtils) extends ParquetLoader {

  val connection = HiveServer2Connection(conf.connectionUrl() + ":" + conf.connectionPort())

  def checkTableExists(table: String, database: String): Boolean = {
    connection.execute("USE %s".format(database))
    var exists: Boolean = false
    connection.executeQuery("SHOW TABLES") { resultSet =>
      val existingTable = resultSet.getString(1).trim
      if (existingTable == table)
        exists = true
    }
    exists
  }

  def createTable(pathString: String, table: String, database: String = "default") {
    val path = new Path(pathString)
    val location = hadoopFs.findAbsolutePath(path)
    val leafPaths = hadoopFs.findSortedLeafPaths(path)

    if (leafPaths.isEmpty)
      error("Could not find parquet files under " + path)

    val tableFields = fieldUtils.findTableFields(leafPaths.last)
    val partitionFields = fieldUtils.findPartitionFields(leafPaths.last)
    val tableWhileImporting = table + "__import"

    connection.execute("CREATE DATABASE IF NOT EXISTS %s".format(database))
    connection.execute("USE %s".format(database))

    createTableWithPartitionFields(location, tableWhileImporting, tableFields, partitionFields)

    connection.execute("DROP TABLE IF EXISTS %s".format(table))
    connection.execute("ALTER TABLE %s RENAME TO %s".format(tableWhileImporting, table))

    if (!partitionFields.isEmpty)
      updateTable(table, database)
  }

  def createTableWithPartitionFields(location: String, table: String, tableFields: List[String],
    partitionFields: List[String]) {

    connection.execute("DROP TABLE IF EXISTS `%s`".format (table))

    val tableClause = "CREATE EXTERNAL TABLE IF NOT EXISTS `%s` (%s)".format(
      table, tableFields.mkString(", "))

    val partitionClause =
      if (partitionFields.isEmpty)
        ""
      else
        " PARTITIONED BY (%s)".format(partitionFields.mkString(" ,"))

    val storedClause = " STORED AS PARQUET LOCATION \"%s\"".format(location)

    connection.execute(tableClause + partitionClause + storedClause)
  }

  def updateTable(table: String, database: String) = {
    connection.execute("MSCK REPAIR TABLE %s".format(table))
  }

  def closeConnection() = connection.close
}
