package com.stripe.herringbone.impala

case class ImpalaClient(host: String, port: Int) {
  lazy val connection = Connection(host, port)

  def execute(raw: String) {
    query(raw){ row =>
      println(row.map { _.raw }.mkString(" "))
    }
  }

  def query(raw: String)(fn: Seq[ImpalaValue] => Unit) {
    println(raw)
    connection.query(raw){ row => fn(row) }
  }
}
