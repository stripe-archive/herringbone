package com.stripe.herringbone.load

trait ParquetLoader {
  def checkTableExists(table: String, db: String): Boolean
  def updateTable(table: String, db: String): Unit
  def createTable(path: String, table: String, db: String): Unit
  def closeConnection(): Unit
}

