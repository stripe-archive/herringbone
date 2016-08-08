package au.com.cba.omnia.herringbone

import au.com.cba.omnia.herringbone.util.ParquetUtils

import java.util.{List => JavaList}
import java.io.DataOutput
import java.io.DataInput

import scala.collection.mutable.MutableList
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.{Configuration,Configured}
import org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.mapreduce.{Job,Mapper}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.util.{Tool,ToolRunner}

import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.`type`.TypeReference

import org.rogach.scallop.ScallopConf

import parquet.example.data.{Group,GroupWriter}
import parquet.hadoop.{BadConfigurationException,ParquetInputFormat,ParquetOutputFormat}
import parquet.hadoop.api.{DelegatingWriteSupport,WriteSupport}
import parquet.hadoop.api.WriteSupport.FinalizedWriteContext
import parquet.hadoop.example.{GroupReadSupport,GroupWriteSupport}

class ParquetCompactConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val inputPath = opt[String](required = true)
  val outputPath = opt[String](descr = "Default is input path with `-compact` appended")
}

class ParquetCompactWriteSupport extends DelegatingWriteSupport[Group](new GroupWriteSupport) {
  var extraMetadata: java.util.Map[String, String] = _

  override def init(configuration: Configuration): WriteSupport.WriteContext = {
    extractMetadata(configuration)
    super.init(configuration)
  }

  override def finalizeWrite(): FinalizedWriteContext = {
    new FinalizedWriteContext(extraMetadata)
  }

  def extractMetadata(configuration: Configuration) = {
    val metadataJson = configuration.get(ParquetCompactWriteSupport.ExtraMetadataKey)
    try {
      extraMetadata = new ObjectMapper().readValue(metadataJson, new TypeReference[java.util.Map[String,String]](){})
    } catch { case e: java.io.IOException =>
      throw new BadConfigurationException("Unable to deserialize extra extra metadata: " + metadataJson, e)
    }
  }
}

object ParquetCompactWriteSupport {
  val ExtraMetadataKey = "herringbone.compact.extrametadata"
}

class CompactJob extends Configured with Tool {
  override def run(arguments: Array[String]) = {
    val conf = new ParquetCompactConf(arguments)
    val inputPath = new Path(conf.inputPath())
    val fs = inputPath.getFileSystem(getConf)
    val outputPathString = conf.outputPath.get.getOrElse(conf.inputPath().stripSuffix("/").concat("-compact"))
    val outputPath = new Path(outputPathString)

    // Pass along metadata (which includes the thrift schema) to the results.
    val metadata = ParquetUtils.readKeyValueMetaData(inputPath)
    val metadataJson = new ObjectMapper().writeValueAsString(metadata)
    getConf.set(ParquetCompactWriteSupport.ExtraMetadataKey, metadataJson)

    if (fs.exists(outputPath)) {
      println(s"Deleting existing $outputPath")
      fs.delete(outputPath, true)
    }

    val job = new Job(getConf)

    FileInputFormat.setInputPaths(job, inputPath)
    FileOutputFormat.setOutputPath(job, outputPath)
    ParquetInputFormat.setReadSupportClass(job, classOf[GroupReadSupport])
    ParquetOutputFormat.setWriteSupportClass(job, classOf[ParquetCompactWriteSupport])
    GroupWriteSupport.setSchema(ParquetUtils.readSchema(inputPath), job.getConfiguration)

    job.setJobName("compact " + conf.inputPath() + " → " + outputPathString)
    job.setInputFormatClass(classOf[CompactGroupInputFormat]);
    job.setOutputFormatClass(classOf[ParquetOutputFormat[Group]])
    job.setMapperClass(classOf[Mapper[Void,Group,Void,Group]])
    job.setJarByClass(classOf[CompactJob])
    job.getConfiguration.setBoolean("mapreduce.job.user.classpath.first", true)
    job.getConfiguration.setBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, false)
    job.getConfiguration.setBoolean(ParquetInputFormat.TASK_SIDE_METADATA, false);
    job.setNumReduceTasks(0)

    if(job.waitForCompletion(true)) 0 else 1
  }
}

object CompactJob {

  def main(args: Array[String]) = {
    val result = ToolRunner.run(new Configuration, new CompactJob, args)
    System.exit(result)
  }
}
