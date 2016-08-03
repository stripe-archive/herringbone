//   Modified work Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.herringbone.util

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