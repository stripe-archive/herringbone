package au.com.cba.omnia.herringbone

import java.util.{List => JavaList}
import java.io.DataOutput
import java.io.DataInput

import scala.collection.mutable.MutableList
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.{InputSplit,Job,JobContext,Mapper,TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import parquet.hadoop.api.ReadSupport
import parquet.hadoop.{ParquetInputFormat,ParquetInputSplit,ParquetOutputFormat,ParquetRecordReader}
import parquet.hadoop.example.{ExampleOutputFormat,GroupReadSupport}
import parquet.hadoop.util.ContextUtil
import parquet.example.data.{Group,GroupWriter}
import parquet.example.data.simple.SimpleGroup


class CompactInputFormat[T](readSupportClass: Class[_ <: ReadSupport[T]]) extends ParquetInputFormat[T](readSupportClass) {

  // Our HDFS block size is 128MB so we'll get pretty close.
  val TARGET = 128 * 1024 * 1024 // 1024MB.

  override def getSplits(context: JobContext): JavaList[InputSplit] = {
    // Limit the splits to 120MB so it's easy to assemble them into 1024MB
    // chunks.  This is not actually reliable. Chunks can come back bigger than
    // 100MB, but it does limit the size of most chunks.
    val conf = ContextUtil.getConfiguration(context)
    conf.set("mapred.max.split.size", (20 * 1024 * 1024).toString)

    val splits = super.getSplits(conf, getFooters(context)).asScala.toList
    val m = if (splits.isEmpty) splits else mergeSplits(splits)
    m.asInstanceOf[List[InputSplit]].asJava
  }

  def mergeSplits(splits: List[ParquetInputSplit]): List[MergedInputSplit] = {
    val sizes = splits.map { _.getLength }
    println(s"""${splits.length} initial splits were generated.
                |  Max: ${mb(sizes.max)}
                |  Min: ${mb(sizes.min)}
                |  Avg: ${mb(sizes.sum.toDouble / sizes.length)}""".stripMargin)

    // TODO: get a CS undergrad to give us better bin packing.
    var buckets = MutableList[MutableList[ParquetInputSplit]](MutableList(splits.head))
    splits.tail.foreach { split =>
      val bucket = buckets.minBy { b => b.map { _.getLength }.sum }
      if ((split.getLength + bucket.map { _.getLength }.sum) < TARGET) {
        bucket += split
      } else {
        buckets += MutableList(split)
      }
    }

    val newSizes = buckets.map { _.map { _.getLength }.sum }.toList
    println(s"""${buckets.length} merged splits were generated.
                |  Max: ${mb(newSizes.max)}
                |  Min: ${mb(newSizes.min)}
                |  Avg: ${mb(newSizes.sum.toDouble / newSizes.length)}""".stripMargin)

    buckets.map { b => new MergedInputSplit(b.toList) }.toList
  }

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): MergedRecordReader[T] = {
    val readSupport = ParquetInputFormat.getReadSupportInstance[T](ContextUtil.getConfiguration(context))
    split match {
      case s: MergedInputSplit => new MergedRecordReader[T](s, context, readSupport)
      case _ => throw new Exception(s"Expected a MergedInputSplit. Found a $split.")
    }
  }

  // Helper for pretty-printing byte values.
  def mb(n: Double): String = {
    val K = 1024
    val M = K * K
    val G = K * M
    if (n < K) f"$n%.2fB"
    else if (n < M) f"${n / K}%.2fK"
    else if (n < G) f"${n / M}%.2fM"
    else f"${n / G}%.2fG"
  }
}

class MergedInputSplit(var splits: List[ParquetInputSplit]) extends InputSplit with Writable {
  def this() = this(List())

  var splitNumber = 0

  def currentSplit: ParquetInputSplit = splits(splitNumber)
  def nextSplit: Option[ParquetInputSplit] = {
    if (splitNumber < splits.length - 1) {
      splitNumber += 1
      Some(currentSplit)
    } else {
      None
    }
  }

  // write and readFields are paired for serialization/deserialization.
  override def write(out: DataOutput) = {
    out.writeInt(splits.length)
    splits.foreach { s => s.write(out) }
  }

  override def readFields(in: DataInput) = {
    val count = in.readInt
    splits = for (i <- List.range(0, count)) yield {
      val s = new ParquetInputSplit
      s.readFields(in)
      s
    }
  }

  override def getLength: Long = splits.map { _.getLength }.sum
  override def getLocations: Array[String] = splits.flatMap { _.getLocations }.toArray
  override def toString = "<MergedInputSplit splits:" + this.splits.length + ">"
}

class MergedRecordReader[T](split: MergedInputSplit,
                            taskContext: TaskAttemptContext,
                            readSupport: ReadSupport[T]) extends ParquetRecordReader[T](readSupport) {
  val totalLength = split.getLength
  var progress = 0L

  override def initialize(split: InputSplit, context: TaskAttemptContext) {
    super.initialize(split.asInstanceOf[MergedInputSplit].currentSplit, context)
  }

  def startNextSplit(split: MergedInputSplit, context: TaskAttemptContext): Boolean = {
    split.nextSplit match {
      case Some(s) => {
        super.initialize(s, context)
        true
      }
      case None => false
    }
  }

  // nextKeyValue is used to ask for the next tuple and returns false when the
  // recordReader has no more tuples. Since we're wrapping multiple splits, and
  // therefore multiple record readers, we detect when the current inernal
  // reader is done and move to the next reader.
  override def nextKeyValue: Boolean = {
    val next = super.nextKeyValue
    if (next) {
      next
    } else {
      super.close
      progress += split.currentSplit.getLength

      if (startNextSplit(split, taskContext)) {
        nextKeyValue
      } else {
        false
      }
    }
  }

  override def toString = "<MergedRecordReader>"
  override def getProgress: Float = progress / totalLength
}


class CompactGroupInputFormat extends CompactInputFormat[Group](classOf[GroupReadSupport]) { }
