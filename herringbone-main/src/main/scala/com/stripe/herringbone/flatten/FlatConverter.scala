package com.stripe.herringbone.flatten

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

import parquet.example.data.Group
import parquet.example.data.GroupWriter
import parquet.example.data.simple.SimpleGroup
import parquet.schema.MessageType

import scala.collection.JavaConversions._

object FlatConverter {
  def groupToTSV(group: Group, flatSchema: MessageType, separator: String, renameId: Boolean): String = {
    val flatGroup = flattenGroup(group, flatSchema, separator, renameId)
    val fieldValues = (0 until flatSchema.getFieldCount).map{ field =>
      val valueCount = flatGroup.getFieldRepetitionCount(field)
      if (valueCount == 0) {
        ""
      } else if (valueCount == 1) {
        escapeString(flatGroup.getValueToString(field, 0))
      } else {
        escapeString(flatGroup.getValueToString(field, 0))
        System.err.println("Warning: Field contains multiple values, extracting only the first")
        System.err.println(flatGroup.toString)
      }
    }
    fieldValues.mkString("\t")
  }

  def constructHeader(schema: MessageType) = {
    schema
      .getPaths()
      .toList
      .map{_(0)}
      .mkString("\t")
  }

  def flattenGroup(group: Group, flatSchema: MessageType, separator: String, renameId: Boolean) = {
    var flatGroup = new SimpleGroup(flatSchema)
    val writer = new GroupWriter(new FlatConsumer(flatGroup, separator, renameId), group.getType)
    writer.write(group)
    flatGroup
  }

  private def escapeString(s: String) = {
    val quote = "\""
    if (s.contains("\t"))
      // This is how pandas escapes tabs and quotes
      quote + s.replace(quote, "\"\"") + quote
    else
      s
  }
}
