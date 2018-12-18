package nimble.internal.api

trait SparkData {
  def underlyingData: Any
}

trait Converter[Catalyst, Scala] {
  def toCatalyst(v: Scala): Catalyst
  def toScala(v: Catalyst): Scala
}

