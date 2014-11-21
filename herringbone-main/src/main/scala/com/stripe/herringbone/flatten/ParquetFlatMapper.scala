package com.stripe.herringbone.flatten

import org.apache.hadoop.mapreduce.Mapper
import parquet.example.data.Group
import parquet.schema.{MessageType,MessageTypeParser}

abstract class ParquetFlatMapper[ValueOut] extends Mapper[Void,Group,Void,ValueOut] {
  var flattenedSchema: MessageType = _
  var separator: String = _
  var renameId: Boolean = _

  override def setup(context: Mapper[Void,Group,Void,ValueOut]#Context) {
    // the schema is stored in the job context when we call ExampleOutputFormat.setSchema
    flattenedSchema = MessageTypeParser.parseMessageType(context.getConfiguration.get("parquet.example.schema"))
    separator = context.getConfiguration.get(ParquetFlatMapper.SeparatorKey)
    renameId = context.getConfiguration.get(ParquetFlatMapper.RenameIdKey) == "true"
  }

  override def map(key: Void, value: Group, context: Mapper[Void,Group,Void,ValueOut]#Context) {
    context.write(key, valueOut(value))
  }

  def valueOut(value: Group): ValueOut
}

object ParquetFlatMapper {
  val SeparatorKey = "herringbone.flatten.separator"
  val RenameIdKey = "herringbone.flatten.rename.id"
}
