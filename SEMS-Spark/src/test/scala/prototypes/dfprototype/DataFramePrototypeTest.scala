package prototypes.dfprototype

import org.apache.spark._
import org.apache.spark.sql._
import prototypes.dfprototype.DataFramePrototype._

import org.junit.Assert._
import org.junit._

class DataFramePrototypeTest {
  
  var simpleDF: DataFrame = null
  var spark: SparkSession = null
  
  @Before def initialSetup {
    
    spark = SparkSession.builder.master("local").appName("Testing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")    
    
    simpleDF = spark.read.json("src/test/resources/basic_data.json")
    simpleDF.show()
  }
  
  @Test def columnNumberTest() {
    assertTrue(simpleDF.columns.length == 4)
  }
  
  @Test def createPairwiseListTest() {
    val list = createPairwiseList(simpleDF)
    val actual = list.toString
    val expected = "Vector((X1,X2), (X1,X3), (X2,X3))"
    assertEquals(expected, actual)
  }
  
}