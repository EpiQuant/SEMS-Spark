package prototypes.dfprototype

import scala.collection.mutable.HashSet
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
  
  @Test def performStepsTest_1 {
    // Tests whether performSteps agrees with the output generated from an R script
    // In this case, there are no entries that will be skipped, i.e. their are no cases
    // where a term is added and later removed from the model
    
    val data = DataFramePrototypeTest.spark.read.json("src/test/resources/performStepsTest_1.json")

    // Initialize the collections case class by adding all of the variables to the not_added collection
    val not_added_init = HashSet() ++ Vector("x1", "x2", "x3", "x4")
    val initial_collection = new StepCollections(not_added = not_added_init)

    val reg = performSteps(DataFramePrototypeTest.spark,
                           df = data,
                           phenotype = "y",
                           initial_collection
                          );
    assertEquals(reg.featureNames.mkString(","), Vector("x2", "x1").mkString(","))
    assertEquals(reg.newestTermsName, "x1")
    assertTrue(reg.newestTermsPValue > 5.04e-05 && reg.newestTermsPValue < 5.06e-05)
    
    val x1Coeff = reg.model.coefficients(1)
    val x2Coeff = reg.model.coefficients(0)
    val intercept = reg.model.intercept
        
    assertTrue(x1Coeff > 1.43142 && x1Coeff < 1.43144)
    assertTrue(x2Coeff > 0.65913 && x2Coeff < 0.65915)
    assertTrue(intercept > 53.02179 && intercept < 53.02181)

/* 
 * The following is the Rscript example that this test should agree with 
 * 
# Data from https://onlinecourses.science.psu.edu/stat501/sites/onlinecourses.science.psu.edu.stat501/files/data/cement.txt
y  <- c(78.5, 74.3, 104.3, 72.5, 93.1, 115.9, 83.8, 113.3, 109.4)
x1 <- c(   7,    1,    11,    1,    2,    21,    1,    11,    10)
x2 <- c(  26,   29,    56,   31,   54,    47,   40,    66,    68)
x3 <- c(   6,   15,     8,   22,   18,     4,   23,     9,     8)
x4 <- c(  60,   52,     6,   44,   22,    26,   34,    12,    12)

summary(lm(y~x1)); summary(lm(y~x2)); summary(lm(y~x3)); summary(lm(y~x4))
# x2 is kept with p-value of 0.00321

summary(lm(y~x2 + x1)); summary(lm(y~x2 + x3)); summary(lm(y~x2 + x4))
# x1 is kept with p-value of 5.05e-05

summary(lm(y~x2 + x1 + x3)); summary(lm(y~x2 + x1 + x4))
# No more things should be added to the model, the final model is
#   y = x2(0.65914) + x1(1.43143) + 53.02180
*/  
  }
  
}