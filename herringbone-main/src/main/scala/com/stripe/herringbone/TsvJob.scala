package com.stripe.herringbone

import com.stripe.herringbone.flatten.{ParquetFlatConf,ParquetFlatMapper,TypeFlattener}
import com.stripe.herringbone.flatten.FlatConverter
import com.stripe.herringbone.util.ParquetUtils

import java.io.{BufferedWriter, OutputStreamWriter}

import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.util._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import org.apache.hadoop.io.Text

import org.rogach.scallop._

import parquet.example.data._
import parquet.example.data.simple._
import parquet.hadoop._
import parquet.hadoop.example._
import parquet.io.api._
import parquet.schema._

import scala.collection.JavaConversions._

class TsvMapper extends ParquetFlatMapper[Text] {
  def valueOut(value: Group) = {
    val tsvLine = FlatConverter.groupToTSV(value, flattenedSchema, separator, renameId) + "\n"
    new Text(tsvLine)
  }
}

class TsvJob extends Configured with Tool {
  override def run(args: Array[String]) = {
    val conf = new ParquetFlatConf(args)
    val fs = FileSystem.get(getConf)
    val inputPath = new Path(conf.inputPath())
    val outputPath = new Path(conf.outputPath())
    val previousPath = conf.previousPath.get.map{new Path(_)}

    val separator = conf.separator()
    getConf.set(ParquetFlatMapper.SeparatorKey, separator)

    val renameId = conf.renameId()
    getConf.set(ParquetFlatMapper.RenameIdKey, renameId.toString)

    if (fs.exists(outputPath)) {
      println(s"Deleting existing $outputPath")
      fs.delete(outputPath, true)
    }

    val flattenedSchema = TypeFlattener.flatten(
      ParquetUtils.readSchema(inputPath, fs),
      previousPath.map { ParquetUtils.readSchema(_, fs) },
      separator,
      renameId
    )

    val jobName = "tsv " + conf.inputPath() + " -> " + conf.outputPath()
    val job = new Job(getConf, jobName)

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    ExampleOutputFormat.setSchema(job, flattenedSchema)

    job.setInputFormatClass(classOf[CompactGroupInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]].asInstanceOf[Class[Nothing]])
    job.setMapperClass(classOf[TsvMapper])
    job.setJarByClass(classOf[TsvJob])
    job.getConfiguration.set("mapreduce.job.user.classpath.first", "true")
    job.setNumReduceTasks(0)

    if (job.waitForCompletion(true)) {
      val headerPath = new Path(conf.outputPath() + "/_header.tsv")
      writeHeader(fs, headerPath, flattenedSchema)
      0
    } else {
      1
    }
  }

  def writeHeader(fs: FileSystem, outputPath: Path, schema: MessageType) {
    val header = FlatConverter.constructHeader(schema)
    val writer = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath, true)))
    writer.write(header)
    writer.write("\n")
    writer.close()
  }
}

object TsvJob {
  def main(args: Array[String]) = {
    val result = ToolRunner.run(new Configuration, new TsvJob, args)
    System.exit(result)
  }
}
