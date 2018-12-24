package nimble.api

import java.util

import org.apache.spark.sql.types.DataType

trait Schema {
  def schema: DataType
}

trait IndexedRecord extends Schema {
  def put[V](i: Int, v: V): Unit
  def get[V](i: Int): V
}

trait GenericRecord extends IndexedRecord {
  def put[V](key: String, v: V): Unit
  def get[V](key: String): V
}

abstract class JList[A] extends util.AbstractList[A] with Schema

abstract class JMap[K, V] extends util.AbstractMap[K, V] with Schema

trait Fn extends Serializable {
  def name: String
  def nullable: Boolean = true
  def deterministic: Boolean = true
  def returnType(args: util.List[DataType]): DataType
}

trait Fn1[-T, +F] extends (T => F) with Fn

trait Fn2[-T1, -T2, +F] extends ((T1, T2) => F) with Fn

trait Fn3[-T1, -T2, -T3, +F] extends ((T1, T2, T3) => F) with Fn

trait Fn4[-T1, -T2, -T3, -T4, +F] extends ((T1, T2, T3, T4) => F) with Fn

trait Fn5[-T1, -T2, -T3, -T4, -T5, +F] extends ((T1, T2, T3, T4, T5) => F) with Fn

trait Fn6[-T1, -T2, -T3, -T4, -T5, -T6, +F] extends ((T1, T2, T3, T4, T5, T6) => F) with Fn
