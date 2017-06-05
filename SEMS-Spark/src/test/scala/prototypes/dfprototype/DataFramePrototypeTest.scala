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
  
  @Test def columnNumberTest {
    assertTrue(DataFramePrototypeTest.simpleDF.columns.length == 4)
  }
  
  @Test def createPairwiseListTest {
    val actual = DataFramePrototypeTest.pairwiseList.toString
    val expected = "Vector((X1,X2), (X1,X3), (X2,X3))"
    assertEquals(expected, actual)
  }
  
  @Test def addPairwiseCombinationsTest {
    DataFramePrototypeTest.dfWithPairs.show()
    
    // Test of the column names   
    assertEquals(DataFramePrototypeTest.dfWithPairs.columns.mkString(", "), "Sample, X1, X2, X3, X1_X2, X1_X3, X2_X3")
    
    // Test of the row values
    val expectedMap = Map("Sample1" -> Row("Sample1", 1.1, 5.4, 9.2,  5.940000000000001, 10.12, 49.68),
                          "Sample2" -> Row("Sample2", 2.3, 6.9, 10.1, 15.87, 23.229999999999997, 69.69),
                          "Sample3" -> Row("Sample3", 3.4, 7.8, 11.1, 26.52, 37.739999999999995, 86.58),
                          "Sample4" -> Row("Sample4", 4.6, 8.4, 12.2, 38.64, 56.11999999999999, 102.48)
                         )
    
    DataFramePrototypeTest.dfWithPairs.collect.toVector.foreach(thisRow => {
      val sample = thisRow(0).toString
      val expected = expectedMap(sample)
      assertEquals(expected, thisRow)
    })
  }
  
  @Test def performRegressionTest_1 {
    /*
     * Simple Linear Regression - one feature column
     */
    val features = Array("X1")
    val labeled_col = "X2"
    val reg = performLinearRegression(features, DataFramePrototypeTest.simpleDF, labeled_col)
    
    assertEquals(reg.featureNames.toVector.toString, "Vector(X1)")
    assertTrue(reg.model.summary.pValues.length == 2)
    
    // Check that the p-values agree with the output given from R's lm method
    val X1coeff = reg.model.summary.pValues(0)
    val intercept = reg.model.summary.pValues(1)
    assertTrue(X1coeff > 0.02010 && X1coeff < 0.02012)
    assertTrue(intercept > 0.00665 && intercept < 0.00667)
  }
  
  @Test def performRegressionTest_2 {
    /*
     * Multiple Linear Regression - two feature columns
     */
    val features = Array("X1", "X2")
    val labeled_col = "X3"
    val reg = performLinearRegression(features, DataFramePrototypeTest.simpleDF, labeled_col)
    
    assertEquals(reg.featureNames.toVector.toString, "Vector(X1, X2)")
    assertTrue(reg.model.summary.pValues.length == 3)
    
    // Check that the p-values agree with the output given from R's lm method
    val X1coeff = reg.model.summary.pValues(0)
    val X2coeff = reg.model.summary.pValues(1)
    val intercept = reg.model.summary.pValues(2)
    assertTrue(X1coeff > 0.0372 && X1coeff < 0.0374)
    assertTrue(X2coeff > 0.1933 && X1coeff < 0.1935)
    assertTrue(intercept > 0.0230 && intercept < 0.0232)
  }
  
  @Test def performSteps_NotAddedTest {
    // Tests whether the item in 
  }
  
}