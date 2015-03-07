package com.stripe.herringbone.load

import com.stripe.herringbone.util.ParquetUtils

import org.apache.hadoop.fs._

import parquet.schema.{ PrimitiveType, Type }
import parquet.schema.PrimitiveType.PrimitiveTypeName
import parquet.schema.PrimitiveType.PrimitiveTypeName._

import scala.collection.JavaConversions._

case class FieldUtils(hadoopFs: HadoopFs, schemaTypeMapper: SchemaTypeMapper) {
  def findPartitionFields(path: Path) = {
    hadoopFs.findPartitions(path).map {
      case (name, example) if (example.forall{_.isDigit}) =>
        "`%s` int".format(name)
      case (name, _) =>
        "`%s` string".format(name)
    }
  }

  def findTableFields(path: Path) = {
    val schema = ParquetUtils.readSchema(path)
    tableFieldsFromSchemaFields(schema.getFields)
  }

  def tableFieldsFromSchemaFields(fields: Seq[Type]) = {
    fields
      .filter { f => f.isPrimitive }
      .map { f =>
        "`%s` %s".format(f.getName, schemaTypeMapper.getSchemaType(f.asInstanceOf[PrimitiveType].getPrimitiveTypeName))
      }.toList
  }
}

trait SchemaTypeMapper {
  def getSchemaType(pt: PrimitiveTypeName): String
}

object ImpalaHiveSchemaTypeMapper extends SchemaTypeMapper {
  def getSchemaType(pt: PrimitiveTypeName) = {
    pt match {
      case BINARY => "STRING"
      case INT32 => "INT"
      case INT64 | INT96 => "BIGINT"
      case DOUBLE => "DOUBLE"
      case BOOLEAN => "BOOLEAN"
      case FLOAT => "FLOAT"
      case FIXED_LEN_BYTE_ARRAY => "BINARY"
    }
  }
}
