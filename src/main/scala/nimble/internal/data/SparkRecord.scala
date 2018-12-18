package nimble.internal.data

import nimble.api.GenericRecord
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StructType}

case class SparkRecord(private val _type: StructType, private val _data: InternalRow)
 extends GenericRecord
   with SparkData {
  /** Set the value of a field given its name. */
  override def put[V](key: String, v: V): Unit = {}

  /** Return the value of a field given its name. */
  override def get[V](key: String): V = null.asInstanceOf[V]

  override def put[V](i: Int, v: V): Unit = {}

  override def get[V](i: Int): V = null.asInstanceOf[V]

  override def underlyingData: Any = ???

  override def schema: DataType = ???
}
