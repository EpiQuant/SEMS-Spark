package prototypes.dfprototype

import scala.annotation.tailrec
import scala.collection.mutable.HashSet
import scala.collection.parallel.ParSeq
import org.apache.spark.ml.regression._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.regression._
import Parser._

case class RegressionOutput(model: LinearRegressionModel,
                            featureNames: Array[String],
                            newestTermsPValue: Double,
                            newestTermsName: String
                           )

case class StepCollections(not_added: HashSet[String],
                           added_prev: HashSet[String] = HashSet(),
                           skipped: HashSet[String] = HashSet()
                          )

object DataFramePrototype {
    
  /*
   *  Maven dependencies:
   *    spark-core_2.11
   *    spark-sql_2.11
   *    spark-mllib_2.11
   */
  
  var threshold = 0.05
        
  def createPairwiseList(df: DataFrame): IndexedSeq[(String, String)] = {
    // Get all of the column names except the first: it is the sample name column
    val col_names = df.columns.toVector.slice(1, df.columns.length)
    
    // Creates a list of all pairwise combinations of each column (names only, does not
    //   actually compute the new values: that will be done per partition)
    for (i <- 0 until col_names.length; j <- i + 1 until col_names.length) yield {
      (col_names(i), col_names(j))
    }
  }
  
  @tailrec
  def addPairwiseCombinations(input_df: DataFrame, input_pairs: IndexedSeq[(String, String)]): DataFrame = {
    
    def addPair(pair: (String, String)): DataFrame = {
      val sqlTrans = new SQLTransformer().setStatement(
          "SELECT *, (" + pair._1 + " * " + pair._2 + ") AS " + pair._1 + "_" + pair._2 + " FROM __THIS__" 
      )
      // Return the input data frame with the new column added
      return sqlTrans.transform(input_df)
    }
    
    // Add the first pair in the pair list to the DataFrame
    val updated_df = addPair(input_pairs(0))
    
    // If that was the last pair to be added, return the final DataFrame
    if (input_pairs.length == 1) return updated_df

    // If not, remove the pair that was just added from the list and call this function again
    else {
      val updated_pairs = input_pairs.slice(1, input_pairs.length)
      addPairwiseCombinations(updated_df, updated_pairs)      
    }
  }

  def performLinearRegression(features: Array[String], df: DataFrame, label_col: String): RegressionOutput = {
        
    val features_name = "features(" + features.mkString(",") + ")"
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol(features_name)
    val output = assembler.transform(df)
    
   // output.show()
    
    val lr = new LinearRegression()
                 .setLabelCol(label_col)
                 .setFeaturesCol(features_name)

    val reg = lr.fit(output)
    
    reg.summary.pValues.foreach(x => print(x + ", "))
    println()
    val newTermsPValue = reg.summary.pValues(features.length - 1)
    
    return RegressionOutput(model = reg,
                            featureNames = features,
                            newestTermsPValue = newTermsPValue,
                            newestTermsName = features(features.length - 1)
                           )
  }

  @tailrec
  def performSteps(spark: SparkSession,
                   df: DataFrame,
                   phenotype: String,
                   collections: StepCollections,
                   prev_best_model: RegressionOutput = null
                   ): RegressionOutput = {
    def mapFunction(collections: StepCollections): ParSeq[RegressionOutput] = {
      // In this implementation, the function is mapped to a collection on the
      //   driver node in a parallel fashion.
      val reg_outputs = collections.not_added.par.toSeq.map(x => {
        // New value always added to the end of the features collection
        val features = collections.added_prev.toArray :+ x
        performLinearRegression(features, df, phenotype)
      })
      return reg_outputs
    }
    
    def reduceFunction(regression_outputs: ParSeq[RegressionOutput]): RegressionOutput = {
      regression_outputs.reduce((x, y) => {
        // The last p-value will be the one for the intercept. We always want the p-value linked to the last
        // term to be added to the model, which is the second to last p-value
        val xPValues = x.model.summary.pValues
        val xPValue = xPValues(xPValues.length - 2)
      
        val yPValues = y.model.summary.pValues
        val yPValue = yPValues(yPValues.length - 2)
      
        if (xPValue <= yPValue) x else y
      })
    }
    
    def constructNamePValuePairs(reg: RegressionOutput): IndexedSeq[(String, Double)] = {
      val map = for (i <- 0 until reg.featureNames.length) yield {
        (reg.featureNames(i), reg.model.summary.pValues(i))
      }
      map
    }
    
    /*
     *  Step 1: find the best regression for those SNPs still under consideration
     */
    // Map generates all of the regression outputs, and reduce finds the best one
    val bestRegression = reduceFunction(mapFunction(collections))
    val pValues = bestRegression.model.summary.pValues
    
    // If the p-value of the newest term does not meet the threshold, return the prev_best_model
    if (bestRegression.newestTermsPValue >= threshold) {
      if (prev_best_model != null) {
        return prev_best_model
      }
      else {
        throw new Exception("No terms could be added to the model at a cutoff of " + threshold)
      }
    }
    else {
      val new_collections = collections.copy()
      
      // Now that the regressions for this round have completed, return any entries in the skipped
      //   category to the not_added category so they may be considered in subsequent iterations
      new_collections.skipped.foreach( x => {
        new_collections.skipped.remove(x)
        new_collections.not_added.add(x)
      })
      
      /*
       * Remove the newest term from the not_added category and put it in the added_prev category
       */
      new_collections.not_added.remove(bestRegression.newestTermsName)
      new_collections.added_prev.add(bestRegression.newestTermsName)
      
      /*
       * Step 2: Check to make sure none of the previously added terms are no longer significant
       * 				 If they are, remove them from consideration in the next round (put them in skipped) and
       *         take them out of the model
       */
      val namePValuePairs = constructNamePValuePairs(bestRegression)
      namePValuePairs.foreach(pair => {
        if (pair._2 >= threshold) {
          // Remove this term from the prev_added collection, and move it to the skipped category
          new_collections.added_prev.remove(pair._1)
          new_collections.skipped.add(pair._1)
        }
      })
      
      if (new_collections.not_added.size == 0) {
        // No more terms that could be added. Return the current best model, unless there are entries
        // in the skipped category. If this is the case, perform one last regression with the current
        // add_prev collection in the model. If something is in the skipped category, the
        // "bestRegression" variable will still have that term included for this iteration
        if (new_collections.skipped.size == 0) return bestRegression
        else {
          return performLinearRegression(new_collections.added_prev.toArray, df, phenotype)
        }
      }
      else {
        performSteps(spark, df, phenotype, new_collections, bestRegression) 
      }
    }
  }
  
  def main(args: Array[String]) = {
     
    
    // 1st argument: SNP file
    // 2nd argument: Phenotype file
    val SNP_file = args(0)
    val pheno_file = args(1)
        
    /*
     *  Define spark session
     */
    //val spark = SparkSession.builder.master("local[2]").appName("Epistasis").getOrCreate()
    val spark = SparkSession.builder.appName("DataFramePrototype").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    /*
     *  Parse the input files
     */

    val snp_df = Parser.CreateDataframe(spark, SNP_file, false, null)
    val pheno_df = Parser.CreateDataframe(spark, pheno_file, false, null)
    
    //val SNP_file = "/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/1106_Markers_NAM_Kernel_Color_Families_Only_for_R_10SNPs.txt"
    //val pheno_file = "/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt"
        
    //val snp_df = Parser.CreateDataframe(spark, SNP_file, true, Array(1,2,3,4))
    //val pheno_df = Parser.CreateDataframe(spark, pheno_file, false, null)
    
    snp_df.show()
    pheno_df.show()
    
    val pairs = createPairwiseList(snp_df)
    val full_df = addPairwiseCombinations(snp_df, pairs)
    
    // Dataframe is made persistent
    val pheno_SNP_df = full_df.join(pheno_df, "Samples").persist()
    
    pheno_SNP_df.show()
    
    val phenotype = "PH1"
    
    //val snp_names = SNPdata._1.map(x => x._1)
    val snp_names = full_df.columns.toVector.slice(1, full_df.columns.length)
    
    val not_added_init = HashSet() ++ snp_names
        
    val initial_collection = new StepCollections(not_added = not_added_init)
    
    val stepsTest = performSteps(spark, pheno_SNP_df, phenotype, initial_collection, null)
    stepsTest.featureNames.foreach(println)
    
  }
}