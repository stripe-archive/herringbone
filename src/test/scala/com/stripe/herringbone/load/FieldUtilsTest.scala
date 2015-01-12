package com.stripe.herringbone.test.load

import com.stripe.herringbone.load.{FieldUtils, HadoopFs, ImpalaHiveSchemaTypeMapper}
import org.apache.hadoop.fs._
import org.scalamock.scalatest.MockFactory
import org.scalatest._
import parquet.schema.{PrimitiveType, Type}

class FieldUtilsTest extends FlatSpec with Matchers with MockFactory {

  "findPartitionFields" should "find the partition field names and types" in {
    val hadoopFs = mock[HadoopFs]
    val path = new Path("path")

    val partitions = List(("day", "123"), ("type", "foo"))
    (hadoopFs.findPartitions _).expects(path).returning(partitions)

    val expected = List("`day` int", "`type` string")
    FieldUtils(hadoopFs, ImpalaHiveSchemaTypeMapper).findPartitionFields(path) should equal (expected)
  }

  "tableFieldsFromSchemaFields" should "find the table fields from the parquet schema" in {
    val hadoopFs = mock[HadoopFs]
    val optional = Type.Repetition.valueOf("OPTIONAL")
    val input = List(
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("BINARY"), "a"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("INT32"), "b"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("INT64"), "c"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("INT96"), "d"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("DOUBLE"), "e"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("BOOLEAN"), "f"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("FLOAT"), "g"),
      new PrimitiveType(optional, PrimitiveType.PrimitiveTypeName.valueOf("FIXED_LEN_BYTE_ARRAY"), "h")
    )

    val expected = List(
      "`a` STRING",
      "`b` INT",
      "`c` BIGINT",
      "`d` BIGINT",
      "`e` DOUBLE",
      "`f` BOOLEAN",
      "`g` FLOAT",
      "`h` BINARY"
    )

    FieldUtils(hadoopFs, ImpalaHiveSchemaTypeMapper).tableFieldsFromSchemaFields(input) should equal (expected)
  }
}
