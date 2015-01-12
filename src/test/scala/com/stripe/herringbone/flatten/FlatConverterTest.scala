package com.stripe.herringbone.test

import com.stripe.herringbone.flatten.{FlatConverter,TypeFlattener}

import org.scalatest._
import org.apache.hadoop.fs.Path

import parquet.example.Paper
import parquet.example.data.simple.SimpleGroup
import parquet.example.data.GroupWriter
import parquet.schema.MessageType
import parquet.schema.PrimitiveType
import parquet.schema.Type.Repetition.OPTIONAL
import parquet.schema.Type.Repetition.REQUIRED
import parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY

import scala.collection.mutable.StringBuilder
import java.io.StringWriter

class FlatConverterTest extends FlatSpec with Matchers {

  def nestedGroupFixture =
    new {
      val group = Paper.r1
      val schema = Paper.schema
      val flatSchema = TypeFlattener.flatten(schema, None, "__", true)
      val flatGroup = FlatConverter.flattenGroup(group, flatSchema, "__", true)
    }

  def flatGroupFixture =
    new {
      val flatSchema =
        new MessageType("Charge",
          new PrimitiveType(REQUIRED, BINARY, "_id"),
          new PrimitiveType(OPTIONAL, BINARY, "email"),
          new PrimitiveType(REQUIRED, BINARY, "merchant")
        )
      val flatGroupMissingFields = new SimpleGroup(flatSchema)
      flatGroupMissingFields.add("_id", "ch_1")
      flatGroupMissingFields.add("merchant", "acct_1")
      val flatGroupAllFields = new SimpleGroup(flatSchema)
      flatGroupAllFields.add("email", "bob@stripe.com")
      flatGroupAllFields.add("merchant", "acct_1")
      flatGroupAllFields.add("_id", "ch_1")
    }

  "groupToTSV" should "convert a flattened group" in {
    val f = nestedGroupFixture
    val groupTSV = FlatConverter.groupToTSV(f.flatGroup, f.flatSchema, "__", true)
    assert(groupTSV == "10\t\t20,40,60")
  }

  "groupToTSV" should "respect schema ordering, handle optional fields" in {
    val f = flatGroupFixture
    val missingTSV = FlatConverter.groupToTSV(f.flatGroupMissingFields, f.flatSchema, "__", true)
    assert(missingTSV == "ch_1\t\tacct_1")
    val allTSV = FlatConverter.groupToTSV(f.flatGroupAllFields, f.flatSchema, "__", true)
    assert(allTSV == "ch_1\tbob@stripe.com\tacct_1")
  }
}

