package com.stripe.herringbone

import com.stripe.herringbone.load._

import org.apache.hadoop.conf._
import org.apache.hadoop.util._

class ParquetLoad extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val conf = new ParquetLoadConf(args)
    val hadoopFs = new HadoopFs()
    val fieldUtils = FieldUtils(hadoopFs, ImpalaHiveSchemaTypeMapper)

    val loader: ParquetLoader = if (conf.hive()) {
      HiveLoader(conf, hadoopFs, fieldUtils)
    } else {
      ImpalaLoader(conf, hadoopFs, fieldUtils)
    }

    if (conf.updatePartitions()) {
      val tableExists = loader.checkTableExists(conf.table(), conf.database())

      (conf.path.get, tableExists) match {
        case (_, true) => loader.updateTable(conf.table(), conf.database())
        case (Some(path), false) => loader.createTable(path, conf.table(), conf.database())
        case (None, false) => {
          println("ERROR - path not specified and table not yet created. Specify path from which to create the table")
          return 1
        }
      }
    } else {
      loader.createTable(conf.path(), conf.table(), conf.database())
    }
    loader.closeConnection

    0
  }
}

object ParquetLoad {
  def main(args: Array[String]) = {
    val result = ToolRunner.run(new Configuration, new ParquetLoad, args)
    System.exit(result)
  }
}
