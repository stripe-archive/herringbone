package com.stripe.herringbone.load

import java.sql.{ Connection, DriverManager, ResultSet }

case class HiveServer2Connection(connectionUrl: String) {
  lazy val connection: Connection = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")
    DriverManager.getConnection(connectionUrl)
  }

  def execute(query: String) {
    try {
      println(query)
      val statement = connection.createStatement
      statement.execute(query)
    } catch {
      case e: Throwable => e.printStackTrace
    }
  }

  def executeQuery(query: String)(fn: ResultSet => Unit) {
    try {
      println(query)
      val statement = connection.createStatement
      val resultSet = statement.executeQuery(query)
      while (resultSet.next) {
        fn(resultSet)
      }
    } catch {
      case e: Throwable => e.printStackTrace
    }
  }

  def close = connection.close
}
