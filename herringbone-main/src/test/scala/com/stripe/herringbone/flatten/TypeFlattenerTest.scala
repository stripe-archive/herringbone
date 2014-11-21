package com.stripe.herringbone.test

import com.stripe.herringbone.flatten.TypeFlattener

import org.scalatest._

import parquet.schema.GroupType
import parquet.schema.MessageType
import parquet.schema.PrimitiveType
import parquet.schema.Type.Repetition.OPTIONAL
import parquet.schema.Type.Repetition.REPEATED
import parquet.schema.Type.Repetition.REQUIRED
import parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY
import parquet.schema.PrimitiveType.PrimitiveTypeName.INT64

class TypeFlattenerTest extends FlatSpec with Matchers {

  "flatten" should "omit the idField in nested fieldname if specified" in {
    val input = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"),
      new GroupType(OPTIONAL, "Page",
          new PrimitiveType(OPTIONAL, BINARY, "_id")))

    val expected = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"),
      new PrimitiveType(OPTIONAL, BINARY, "Page"))

    val result = TypeFlattener.flatten(input, None, "__", true)
    assert(expected == result)
  }

  "flatten" should "not omit the idField in nested fieldname if none is specified" in {
    val input = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"),
      new GroupType(OPTIONAL, "Page",
          new PrimitiveType(OPTIONAL, BINARY, "_id")))

    val expected = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"),
      new PrimitiveType(OPTIONAL, BINARY, "Page___id"))

    val result = TypeFlattener.flatten(input, None, "__", false)
    assert(expected == result)
  }

  "flatten" should "not include repeated groups" in {
    val input = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"),
      new GroupType(REPEATED, "Nope",
          new PrimitiveType(REPEATED, INT64, "Never")))

    val expected = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "_id"))

    val result = TypeFlattener.flatten(input, None, "__", true)
    assert(expected == result)
  }

  "flatten" should "set all fields as optional" in {
    val input = new MessageType("Document",
      new GroupType(OPTIONAL, "Yep",
          new GroupType(REQUIRED, "Grouped",
              new PrimitiveType(REQUIRED, BINARY, "Yes"),
              new PrimitiveType(REPEATED, BINARY, "Maybe")),
          new PrimitiveType(OPTIONAL, BINARY, "Sometimes")))

    val expected = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "Yep__Grouped__Yes"),
      new PrimitiveType(OPTIONAL, BINARY, "Yep__Grouped__Maybe"),
      new PrimitiveType(OPTIONAL, BINARY, "Yep__Sometimes"))

    val result = TypeFlattener.flatten(input, None, "__", true)
    assert(expected == result)
  }

  "flatten" should "preserve the order of previously flattened fields" in {
    val input = new MessageType("Document",
      new PrimitiveType(REQUIRED, BINARY, "Old__Two"),
      new GroupType(OPTIONAL, "New",
        new PrimitiveType(REQUIRED, BINARY, "One")),
      new PrimitiveType(REQUIRED, BINARY, "Old__One"))

    val old = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "Old__One"),
      new PrimitiveType(OPTIONAL, BINARY, "Old__Two"))

    val expected = new MessageType("Document",
      new PrimitiveType(OPTIONAL, BINARY, "Old__One"),
      new PrimitiveType(OPTIONAL, BINARY, "Old__Two"),
      new PrimitiveType(OPTIONAL, BINARY, "New__One"))

    val result = TypeFlattener.flatten(input, Some(old), "__", true)
    assert(expected == result)
  }
}
