package com.stripe.herringbone.flatten

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

import parquet.example.data._
import parquet.example.data.simple._
import parquet.hadoop._
import parquet.hadoop.example._
import parquet.io.api._
import parquet.schema._

class FlatConsumer(output: Group, separator: String, renameId: Boolean) extends RecordConsumer {

  case class StackFrame(field: String, var values: List[Binary])
  var stack = List[StackFrame]()
  // Impala stops working after a field becomes too long. The docs
  // indicate that we should have 32k. However, a binary search on a
  // too-long field yielded 6776 as the maximum working value.
  val MaxStringBytes = 6776

  def startMessage {}
  def endMessage {}
  def startGroup {}
  def endGroup {}

  def startField(field: String, index: Int) {
    stack ::= StackFrame(field, Nil)
  }

  def endField(field: String, index: Int) {
    if (stack.head.values.size == 1) {
      withField{name => output.add(name, stack.head.values.head)}
    } else if (stack.head.values.size > 1) {
      withField {name =>
        val joined = Binary.fromString(
          stack
            .head
            .values
            .reverse
            .map{_.toStringUsingUTF8}
            .mkString(",")
            .replace("\t", " ")
        )
        val truncated = truncate(joined, MaxStringBytes)
        output.add(name, truncated)
      }
    }
    stack = stack.tail
  }

  def addInteger(value: Int) {
    writeField{Binary.fromString(value.toString)}{name => output.add(name, value)}
  }

  def addLong(value: Long) {
    writeField{Binary.fromString(value.toString)}{name => output.add(name, value)}
  }

  def addBoolean(value: Boolean) {
    writeField{Binary.fromString(value.toString)}{name => output.add(name, value)}
  }

  def truncate(value: Binary, length: Integer): Binary = {
    if (value.length <= length) {
      value
    } else {
      val bytesTruncated = new Array[Byte](length)
      value.toByteBuffer.get(bytesTruncated, 0, length)
      Binary.fromByteArray(bytesTruncated)
    }
  }

  def addBinary(value: Binary) {
    // Truncate strings so Impala doesn't break
    val truncated = truncate(value, MaxStringBytes)
    writeField(truncated){name => output.add(name, truncated)}
  }

  def addFloat(value: Float) {
    writeField{Binary.fromString(value.toString)}{name => output.add(name, value)}
  }

  def addDouble(value: Double) {
    writeField{Binary.fromString(value.toString)}{name => output.add(name, value)}
  }

  def withField(fn: String=>Unit) {
    val path = if (TypeFlattener.omitIdField(stack.head.field, stack.size, renameId))
      stack.tail
    else
      stack

    val name = path.reverse.map{_.field}.mkString(separator)
    if(output.getType.containsField(name))
      fn(name)
  }

  def writeField(binRep: =>Binary)(fn: String => Unit) {
    withField{name =>
      val fieldType = output.getType.getType(name)
      if(fieldType.asInstanceOf[PrimitiveType].getPrimitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY)
        stack.head.values ::= binRep
      else
        fn(name)
    }
  }
}
