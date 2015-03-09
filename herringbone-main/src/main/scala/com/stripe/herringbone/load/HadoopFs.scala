package com.stripe.herringbone.load

import com.stripe.herringbone.util.ParquetUtils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.util._

class HadoopFs {
  def findAbsolutePath(path: Path) = {
    path.getFileSystem(new Configuration).getFileStatus(path).getPath.toUri.toString
  }

  def findSortedLeafPaths(path: Path): List[Path] =
    findLeafPaths(path).sortBy{case (path,time) => time}.map{_._1}

  def findLeafPaths(path: Path): List[(Path,Long)] = {
    val fs = path.getFileSystem(new Configuration)
    val parquetFileStatuses = fs.listStatus(path, ParquetUtils.parquetFilter)
    if (parquetFileStatuses.size > 0)
      List((path, parquetFileStatuses.head.getModificationTime))
    else {
      fs.listStatus(path, ParquetUtils.partitionFilter)
        .toList
        .map{_.getPath}
        .flatMap{findLeafPaths(_)}
    }
  }

  def findPartitions(path: Path) = {
    path.toUri.getPath.split("/")
      .filter{_.contains("=")}
      .map{segment =>
        val parts = segment.split("=")
        (parts(0), parts(1))
      }.toList
  }
}
