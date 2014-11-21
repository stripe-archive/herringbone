package com.stripe.herringbone.impala

import java.text.SimpleDateFormat

case class ImpalaValue(raw: String, fieldName: String, fieldType: String) {
  lazy val convertedValue = convertRawValue(raw)

  private def convertRawValue(raw: String): Option[Any] = {
    if (raw == "NULL") {
      None
    } else {
      val converted = fieldType match {
        case "string" => raw
        case "boolean" => convertBoolean(raw)
        case "tinyint" | "smallint" | "int" | "bigint" => raw.toInt
        case "double" | "float" | "decimal" => raw.toDouble
        case "timestamp" => convertTimestamp(raw)
        case _ => throw ParsingException("Unknown type: " + fieldType)
      }
      Some(converted)
    }
  }

  private def convertBoolean(raw: String) = {
    try {
      raw.toBoolean
    } catch {
      case e: java.lang.IllegalArgumentException =>
        throw ParsingException("Invalid value for boolean: " + raw)
    }
  }

  private def convertTimestamp(raw: String) = {
    val formatStr = if (raw.indexOf(".") == -1) {
      "YYYY-MM-DD HH:MM:SS"
    } else {
      "YYYY-MM-DD HH:MM:SS.sssssssss"
    }

    val dateFormat = new SimpleDateFormat(formatStr)
    dateFormat.parse(raw)
  }
}
