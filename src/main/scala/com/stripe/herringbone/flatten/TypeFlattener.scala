package com.stripe.herringbone.flatten

import parquet.schema._
import java.util.{List=>JList}
import scala.collection.JavaConverters._

class TypeFlattener(separator: String, renameId: Boolean) extends TypeConverter[List[Type]] {
    def convertPrimitiveType(path: JList[GroupType], primitiveType: PrimitiveType) = {
      val typeName =
        if(TypeFlattener.isRepeated(primitiveType))
          PrimitiveType.PrimitiveTypeName.BINARY
        else
          primitiveType.getPrimitiveTypeName

      val types = if (TypeFlattener.omitIdField(primitiveType.getName, path.size, renameId))
        path.asScala.tail
      else
        (path.asScala.tail :+ primitiveType)

      val name = types.map{_.getName}.mkString(separator)
      List(new PrimitiveType(Type.Repetition.OPTIONAL, typeName, primitiveType.getTypeLength, name))
    }

    def convertGroupType(path: JList[GroupType], groupType: GroupType, children: JList[List[Type]]) = {
      if(TypeFlattener.isRepeated(groupType))
        Nil
      else
        flatten(children)
    }

    def convertMessageType(messageType: MessageType, children: JList[List[Type]]) = flatten(children)

    def flatten(children: JList[List[Type]]) = children.asScala.flatten.toList
}

object TypeFlattener {
  def flatten(messageType: MessageType,
    previousMessageType: Option[MessageType],
    separator: String,
    renameId: Boolean) = {
    val flattened = messageType.convertWith(new TypeFlattener(separator, renameId))
    val fieldsToUse = previousMessageType match {
      case Some(prevMessageType) => {
        // if passed a previous flattened schema, preserve that field ordering,
        // and append any new fields
        val prevFields = prevMessageType.getFields.asScala.toList
        prevFields ::: flattened.filterNot{prevFields.contains(_)}
      }
      case None => flattened
    }
    new MessageType(messageType.getName, fieldsToUse.asJava)
  }

  def isRepeated(t: Type) = t.isRepetition(Type.Repetition.REPEATED)

  def omitIdField(fieldName: String, numberOfFields: Integer, renameId: Boolean) = {
    renameId && Seq("id", "_id").contains(fieldName) && numberOfFields > 1
  }
}
