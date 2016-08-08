package com.stripe.herringbone.util

import org.apache.hadoop.conf._
import org.apache.hadoop.util._
import org.apache.hadoop.fs._

import parquet.hadoop.ParquetFileReader

object ParquetUtils {
  def getParquetMetadata(path: Path) = {
    // Just use the first parquet file to figure out the impala fields
    // This also dodges the problem of any non-parquet files stashed
    // in the path.
    val fs = path.getFileSystem(new Configuration)
    val parquetFileStatuses = fs.listStatus(path, parquetFilter)
    val representativeParquetPath = parquetFileStatuses.head.getPath

    val footers = ParquetFileReader.readFooters(new Configuration, representativeParquetPath)
    footers.get(0).getParquetMetadata
  }

  def readSchema(path: Path) = {
    getParquetMetadata(path).getFileMetaData.getSchema
  }

  def readKeyValueMetaData(path: Path) = {
    getParquetMetadata(path).getFileMetaData.getKeyValueMetaData
  }

  val parquetFilter = new PathFilter {
    def accept(path: Path) = path.getName.endsWith(".parquet")
  }

  val partitionFilter = new PathFilter {
    def accept(path: Path) = path.getName.contains("=")
  }
}
