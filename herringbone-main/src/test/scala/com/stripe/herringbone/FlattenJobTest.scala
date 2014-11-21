package com.stripe.herringbone.test

import com.stripe.herringbone.flatten._
import org.scalatest._
import parquet.example.Paper
import parquet.io.api.Binary

class FlattenJobTest extends FlatSpec with Matchers {
  def toBinary(x: Array[Byte]) = Binary.fromByteArray(x)

  "truncate" should "truncate to correct length" in {
    val consumer = new FlatConsumer(Paper.r1, "__", false)
    val bytes = toBinary(Array[Byte](1,2,3,4))
    assert(consumer.truncate(bytes, 3).getBytes().sameElements(Array[Byte](1,2,3)))
  }

  "truncate" should "not truncate if unnecessary" in {
    val consumer = new FlatConsumer(Paper.r1, "__", false)
    val bytes = toBinary(Array[Byte](1,2,3,4))
    assert(consumer.truncate(bytes, 8) == bytes)
  }
}
