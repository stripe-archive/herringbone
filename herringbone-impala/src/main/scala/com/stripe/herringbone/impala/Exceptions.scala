package com.stripe.herringbone.impala

case class ConnectionException(message: String) extends Exception
case class CursorException(message: String) extends Exception
case class InvalidQueryException(message: String) extends Exception
case class ParsingException(message: String) extends Exception

