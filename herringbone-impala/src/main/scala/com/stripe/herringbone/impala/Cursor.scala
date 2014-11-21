package com.stripe.herringbone.impala

import org.apache.hadoop.hive.metastore.api.FieldSchema

import com.cloudera.impala.thrift.ImpalaService.{Client => ClouderaImpalaClient}
import com.cloudera.beeswax.api._

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

case class Cursor(handle: QueryHandle, client: ClouderaImpalaClient) {
  var done = false
  var isOpen = true
  var rowBuffer = ArrayBuffer.empty[Seq[ImpalaValue]]
  val bufferSize = 1024
  private lazy val metadata: ResultsMetadata = client.get_results_metadata(handle)

  def foreach(fn: Seq[ImpalaValue] => Unit) = {
    var row = fetchRow
    while (row.isDefined) {
      fn(row.get)
      row = fetchRow
    }
  }

  def fetchRow: Option[Seq[ImpalaValue]] = {
    if (rowBuffer.isEmpty) {
      if (done) {
        None
      } else {
        fetchMore
        fetchRow
      }
    } else {
      val row = rowBuffer.head
      rowBuffer = rowBuffer.tail
      Some(row)
    }
  }

  // Close the cursor on the remote server. Once a cursor is closed, you
  // can no longer fetch any rows from it.
  def close = {
    if (!isOpen) {
      isOpen = false
      client.close(handle)
    }
  }

  // Returns true if there are any more rows to fetch.
  def hasMore = !done || !rowBuffer.isEmpty

  def runtime_profile = client.GetRuntimeProfile(handle)

  private def fetchMore = {
    while (!done && rowBuffer.size < bufferSize) {
      fetchBatch
    }
  }

  private def fetchBatch = {
    if (!isOpen) throw CursorException("Cursor has expired or been closed")

    try {
      val response = client.fetch(handle, false, bufferSize)
      validateQueryState(client.get_state(handle))

      val rows  = response.data.map { row => parseRow(row) }
      rowBuffer ++= rows

      if (!response.has_more) {
        done = true
        close
      }
    } catch {
      case e: BeeswaxException => {
        isOpen = false
        throw e
      }
      case e: Exception => throw e
    }
  }

  private def parseRow(row: String) = {
    val fields = row.split(metadata.delim)

    metadata.schema.getFieldSchemas.zip(fields).map { case(schema, rawValue) =>
      ImpalaValue(rawValue, schema.getName, schema.getType)
    }
  }

  private def validateQueryState(state: QueryState) = {
    if (state == QueryState.EXCEPTION) {
      close
      throw CursorException("The query was aborted")
    }
  }
}
