package nimble

import com.google.common.collect.ImmutableList
import nimble.internal.api.SparkData
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types._
import org.testng.annotations.Test

class TestDataTypes {

  @Test
  def nestedWrap(): Unit = {
    // map of
  }

  @Test
  def test(): Unit = {
    val dt = createStructType(
      ImmutableList.of(
        createStructField("a", DataTypes.StringType, true),
        createStructField("b", DataTypes.IntegerType, true)))
    val r = Generator.genData(dt)
    CatalystVerifier.verify(r.asInstanceOf[SparkData].underlyingData, dt)
    /*
    val listOfInts = DataFactory.emptyList[Int](IntegerType)
    listOfInts.add(1)
    assertEquals(ImmutableList.of(1), listOfInts)

    val listOfDecimals = DataFactory.emptyList[BigDecimal](DataTypes.createDecimalType())
    listOfDecimals.add(BigDecimal.ONE)
    assertTrue(listOfDecimals.contains(BigDecimal.ONE))

    val listOf

    val record = DataFactory.emptyRecord(
      createStructType(
        ImmutableList.of(
          createStructField("a", StringType, true),
          createStructField("b", IntegerType, true))))

    record.put("a", "astring")
    record.put("b", 1)
    */
  }
}
