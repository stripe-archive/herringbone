package com.stripe.herringbone.impala

import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol

import com.cloudera.impala.thrift.ImpalaService.{Client => ClouderaImpalaClient}
import com.cloudera.beeswax.api._

import scala.annotation.tailrec
import scala.collection.JavaConversions._

case class Connection(host: String, port: Int) {
  var isOpen = false
  val logContext = "herringbone-impala"
  lazy val socket = new TSocket(host, port)
  lazy val client = new ClouderaImpalaClient(new TBinaryProtocol(socket))

  open

  def open = {
    if (!isOpen) {
      socket.open
      client.ResetCatalog
      isOpen = true
    }
  }

  def close = {
    if (isOpen) {
      socket.close
      isOpen = false
    }
  }

  // Refresh the metadata store.
  def refresh = {
    if (!isOpen) throw ConnectionException("Connection closed")
    client.ResetCatalog
  }

  // Perform a query, and pass in a function that will be called with each
  // row of the results
  def query(raw: String)(fn: Seq[ImpalaValue] => Unit) {
    val cursor = execute(raw)
    cursor.foreach { row => fn(row) }
    cursor.close
  }

  // Perform a query and return a cursor for iterating over the results.
  // You probably want to call cursor.close when you're done with it.
  def execute(raw: String): Cursor = {
    if (!isOpen) throw ConnectionException("Connection closed")
    validateQuery(raw)

    val query = new Query
    query.query = raw

    val handle = client.executeAndWait(query, logContext)
    Cursor(handle, client)
  }

  private def validateQuery(raw: String) = {
    val words = raw.split("\\s+")
    if (words.isEmpty) throw InvalidQueryException("Empty query")
  }
}
