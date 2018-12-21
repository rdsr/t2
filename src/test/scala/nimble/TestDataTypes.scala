package nimble

import java.util.function.Consumer

import com.google.common.collect.ImmutableList
import nimble.api.DataFactory
import nimble.internal.api.SparkData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DataTypes._
import org.testng.Assert
import org.testng.annotations.Test

class TestDataTypes {
  @Test
  def test(): Unit = {
    val l = DataFactory.emptyList[Int](IntegerType)
    val arrayData = l.asInstanceOf[SparkData].underlyingData
    l.add(1)
    l.add(2)
    l.add(3)
    Assert.assertEquals(ImmutableList.of(1,2,3), l)

    val record = DataFactory.emptyRecord(
      createStructType(
        ImmutableList.of(
          createStructField("a", StringType, true),
          createStructField("b", IntegerType, true))))

    record.put("a", "astring")
    record.put("b", 1)
    println(record.get("a"))
    println(record.get("b"))
  }
}
