package prototypes.rddprototype

import scala.collection.mutable.HashSet
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast.Broadcast
import org.junit.Assert._
import org.junit._

class RDDPrototypeTest {
  /** Maximum acceptable difference between expected and actual float values used in Assert statements */
  final val DELTA = 1e-5
  
  var spark: SparkSession = SparkSession.builder.master("local").appName("Testing").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  val stepDataX = List( ("x1", Vector(7.0, 1, 11, 1, 2, 21, 1, 11, 10)      ),
                        ("x2", Vector(26.0, 29, 56, 31, 54, 47, 40, 66, 68) ),
                        ("x3", Vector(6.0, 15, 8, 22, 18, 4, 23, 9, 8)      ),
                        ("x4", Vector(60.0, 52, 6, 44, 22, 26, 34, 12, 12)  )
                      )
                      
  val stepDataY = List( ("pheno", Vector(78.5, 74.3, 104.3, 72.5, 93.1, 115.9, 83.8, 113.3, 109.4)) )
  
  var broadcastXTable = spark.sparkContext.broadcast(stepDataX.toMap)
  var broadcastYTable = spark.sparkContext.broadcast(stepDataY.toMap)
  
  val stepDataRDD = spark.sparkContext.parallelize(stepDataX)

  @Test def createPairwiseListTest {
    val actual = RDDPrototype.createPairwiseList(List("x1","x2", "x3"))
    val expected = Seq(("x1", "x2"), ("x1", "x3"), ("x2", "x3"))
    assertEquals(expected, actual)
  }

  @Test def createPairwiseColumnTest {
    val actual = RDDPrototype.createPairwiseColumn(("x1", "x2"), broadcastXTable)
    val expected = ("x1_x2",
                    Vector(182.0, 29.0, 616.0, 31.0, 108.0, 987.0, 40.0, 726.0, 680.0)
                   )
    assertEquals(expected, actual)
  }

  @Test def performStepsTest_simple {
    // Tests whether performSteps agrees with the output generated from an R script
    // In this case, there are no entries that will be skipped, i.e. their are no cases
    // where a term is added and later removed from the model
    
    // Initialize the collections case class by adding all of the variables to the not_added collection
    val not_added_init = HashSet() ++ Vector("x1", "x2", "x3", "x4")
    val initial_collection = new StepCollections(not_added = not_added_init)
    
    val reg = RDDPrototype.performSteps(spark.sparkContext,
                                        stepDataRDD,
                                        broadcastYTable,
                                        "pheno",
                                        initial_collection,
                                        0.05,
                                        0.05                                     
                                       )                                     
                                       
    assertEquals(Vector("x2", "x1"), reg.xColumnNames.toVector)
    assertEquals("x1", reg.xColumnNames.last)

    val x1PValue = reg.pValueMap("x1")
    val x2PValue = reg.pValueMap("x2")
    val interceptPValue = reg.pValues.last
    
    assertEquals(5.05e-05, x1PValue, DELTA)
    assertEquals(3.42e-05, x2PValue, DELTA)
    assertEquals(9.52e-07, interceptPValue, DELTA)

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

  @Test def performStepsTest_skipped {
     /*  This tests two things:
     *    1. If a previously added term is no longer significant when another term is added,
     *         it is removed from the added_prev collection and placed in the skipped category
     *    2. If there are no more terms in the not_added collection and the skipped collection
     *         is not empty, a final regression model will be generated that does not include the
     *         term that was removed from the model in the final step
     */
    
    // For this test, we will start with a term that will not be significant, "x3", in the prev_added
    //   collection, so it will be found and skipped after a significant term, "x1", is added
    
    // In addition, since there will be no others terms that can be added to the model and there
    //   will be an entry in the skipped category, this test whether the final model (without the
    //   skipped term) is returned
    val not_added_init = HashSet() ++ Vector("x1")
    val added_prev_init = HashSet() ++ Vector("x3")
    val initial_collection = new StepCollections(not_added = not_added_init,
                                                 added_prev = added_prev_init
                                                )
    
    val reg = RDDPrototype.performSteps(spark.sparkContext,
                                        stepDataRDD,
                                        broadcastYTable,
                                        "pheno",
                                        initial_collection,
                                        0.05,
                                        0.05
                                       )
    
    assertEquals(Vector("x1"), reg.xColumnNames.toVector)
    assertEquals("x1", reg.xColumnNames.last)

    val x1PValue = reg.pValueMap("x1")
    val interceptPValue = reg.pValues.last
    
    assertEquals(0.00515, x1PValue, DELTA)
    assertEquals(1.6e-06, interceptPValue, DELTA)

   /* 
    * The following is the Rscript example that this test should agree with 
    * 
      # Data from https://onlinecourses.science.psu.edu/stat501/sites/onlinecourses.science.psu.edu.stat501/files/data/cement.txt
			y <- c(78.5, 74.3, 104.3, 72.5, 93.1, 115.9, 83.8, 113.3, 109.4)
			x1 <- c(7, 1, 11, 1, 2, 21, 1, 11, 10)
			x3 <- c(6, 15, 8, 22, 18, 4, 23, 9, 8)

			# In this test, we start with a term already included in the model
			# This term, x3, should be insignificant and removed from the model
			# and added to the skipped category after x1 is added

			summary(lm(y~x3 + x1))
			# x1 will be added and x3 should be removed as it has a p-value of 0.6205

			summary(lm(y~x1))
			# The final model should be y = x1(2.1223) + 78.5724
		*/
  }
  
   @Test def performStepsTest_skippedReturned {
     /*
     * This tests that items in the skipped category are returned to the not_added collection after
     *   one iteration
     * 
     * This works by starting with x1 in the skipped category. So x2 will be added in the
     *   next iteration, provided that it has been removed from the skipped category
     */
    val not_added_init = HashSet() ++ Vector("x1")
    val skipped_init = HashSet() ++ Vector("x2")
    val initial_collection = new StepCollections(not_added = not_added_init,
                                                 skipped = skipped_init
                                                )
    
    val reg = RDDPrototype.performSteps(spark.sparkContext,
                                        stepDataRDD,
                                        broadcastYTable,
                                        "pheno",
                                        initial_collection,
                                        0.05,
                                        0.05
                                       )
    
    assertEquals(Vector("x1", "x2"), reg.xColumnNames.toVector)
    assertEquals("x2", reg.xColumnNames.last)

    val x1PValue = reg.pValueMap("x1")
    val x2PValue = reg.pValueMap("x2")
    val interceptPValue = reg.pValues.last
    
    assertEquals(5.05e-05, x1PValue, DELTA)
    assertEquals(3.42e-05, x2PValue, DELTA)
    assertEquals(9.52e-07, interceptPValue, DELTA)
    /*
    */
   /* 
    * The following is the Rscript example that this test should agree with 
    * 
      # Data from https://onlinecourses.science.psu.edu/stat501/sites/onlinecourses.science.psu.edu.stat501/files/data/cement.txt
			y <- c(78.5, 74.3, 104.3, 72.5, 93.1, 115.9, 83.8, 113.3, 109.4)
			x1 <- c(7, 1, 11, 1, 2, 21, 1, 11, 10)
			x2 <- c(26, 29, 56, 31, 54, 47, 40, 66, 68)

			summary(lm(y~x1))
						
			# Next iteration, after x2 in skipped is added back into consideration			
			summary(lm(y~x1 + x2))
			

			summary(lm(y~x3 + x1))
			# x1 will be added and x3 should be removed as it has a p-value of 0.6205

		*/
  }
}