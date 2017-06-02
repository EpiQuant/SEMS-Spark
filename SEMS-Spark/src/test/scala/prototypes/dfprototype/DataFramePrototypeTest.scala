package prototypes.dfprototype

import org.apache.spark._
import org.apache.spark.sql._
import prototypes.dfprototype.DataFramePrototype._
import org.junit.Assert._
import org.junit._

object DataFramePrototypeTest {
  
  var simpleDF: DataFrame = null
  var spark: SparkSession = null
  var pairwiseList: IndexedSeq[(String, String)] = null
  var dfWithPairs: DataFrame = null
  
  @BeforeClass def initialSetup {
    
    spark = SparkSession.builder.master("local").appName("Testing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")    
    
    simpleDF = spark.read.json("src/test/resources/basic_data.json")
    simpleDF.show()
  
    pairwiseList = createPairwiseList(simpleDF)
    dfWithPairs = addPairwiseCombinations(simpleDF, pairwiseList)
  }
}

class DataFramePrototypeTest {
  
  @Test def columnNumberTest() {
    assertTrue(DataFramePrototypeTest.simpleDF.columns.length == 4)
  }
  
  @Test def createPairwiseListTest() {
    val actual = DataFramePrototypeTest.pairwiseList.toString
    val expected = "Vector((X1,X2), (X1,X3), (X2,X3))"
    assertEquals(expected, actual)
  }
  
  @Test def addPairwiseCombinationsTest() {
    DataFramePrototypeTest.dfWithPairs.show()
    
    // Test of the column names   
    assertEquals(DataFramePrototypeTest.dfWithPairs.columns.mkString(", "), "Sample, X1, X2, X3, X1_X2, X1_X3, X2_X3")
    
    // Test of the row values
    val expectedMap = Map("Sample1" -> Row("Sample1", 1, 5, 9, 5, 9,45), "Sample2" -> Row("Sample2", 2, 6,10,12,20,60),
                          "Sample3" -> Row("Sample3", 3, 7,11,21,33,77), "Sample4" -> Row("Sample4", 4, 8,12,32,48,96)
                         )
    
    DataFramePrototypeTest.dfWithPairs.collect.toVector.foreach(thisRow => {
      val sample = thisRow(0).toString
      val expected = expectedMap(sample)
      assertEquals(expected, thisRow)
    })
  }
  
}