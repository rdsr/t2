package t2.api

import java.util

import org.apache.spark.sql.api.java._
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

abstract class GenericList[A] extends util.AbstractList[A] with Schema

abstract class GenericMap[K, V] extends util.AbstractMap[K, V] with Schema

trait Fn0[F] extends UDF0[F] with Fn

trait Fn1[T, F] extends UDF1[T, F] with Fn

trait Fn2[T1, T2, F] extends UDF2[T1, T2, F] with Fn

trait Fn3[T1, T2, T3, F] extends UDF3[T1, T2, T3, F] with Fn

trait Fn4[T1, T2, T3, T4, F] extends UDF4[T1, T2, T3, T4, F] with Fn

trait Fn5[T1, T2, T3, T4, T5, F] extends UDF5[T1, T2, T3, T4, T5, F] with Fn

trait Fn6[T1, T2, T3, T4, T5, T6, F] extends UDF6[T1, T2, T3, T4, T5, T6, F] with Fn
