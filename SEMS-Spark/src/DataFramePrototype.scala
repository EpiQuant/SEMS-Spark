import scala.io.Source
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
  
  def parseInputFileWithSampleNames(input: String): (Vector[(String, Vector[Double])], Vector[String]) = {
    // Returns tuple with Vector(SNP_name -> values) 
    //   and the list of sample names in the order the values are recorded
    
    // Open file handle
    val buffSource = Source.fromFile(input)
    
    // Grab the header line (and split it)
    val header: Vector[String] = buffSource.getLines.next.split("\t").toVector
    // Grab all of the other lines (and split them)
    val lines: Vector[Vector[String]] =
      buffSource.getLines.toVector.map(line => line.split("\t").toVector)
    
    buffSource.close()
    
    /*
     * The "columns" variable is a collection of collections: for each column, it has a collection
     *   of the values for that column
     */
    // For each column ...
    val columns: IndexedSeq[Vector[Double]] = for (i <- 1 until header.length) yield {
      // ... Go through each line and return the value for that column
      lines.map(x => x(i).toDouble) 
    }
   
    // Turn columns into collection of Tuples with (ColumnName, ValueList)
    val names_values: IndexedSeq[(String, Vector[Double])] =
      // i + 1 in header because ID column gone in "columns" but still present in "header"
      for (i <- 0 until columns.length) yield (header(i + 1), columns(i))
      
    val names = lines.map(x => x(0))
    
    return (names_values.toVector, names) 
  }
  
  private[this] def createSchema(sorted_snp_names: Array[String]): StructType = {
    
    val SNP_headers = sorted_snp_names.map(name => (StructField(name, DoubleType, nullable = true))).toList
    
    // prepends the samples column header to the snp headers list
    val schema_list = StructField("Samples", StringType, nullable = true) :: SNP_headers;
    
    val schema = StructType(schema_list)
    return schema
  }
  
  private[this] def createRow(sample_names_order: Vector[String],
                sample_position: Int,
                sorted_snp_names: Array[String],
                inputRDD: rdd.RDD[(String, Vector[Double])]
      ): Row = {
    val sample_name = sample_names_order(sample_position)
    
    // These values will be in the order matching that of the sample_names_order
    val sample_values = sorted_snp_names.map( snp_name => { 
      // Since we know that there is only one entry for each key in the RDD, we just grab the first item in
      // the collection returned from the lookup function and extract the value we need from that
      val samples_snp_value = inputRDD.lookup(snp_name)(0)(sample_position)
      // This value is returned
      samples_snp_value
    })
    
    val output = Row.fromSeq(sample_name +: sample_values)
    return output
  }
  
  def createDataFrame(session: SparkSession, inputRDD: rdd.RDD[(String, Vector[Double])], 
                      sample_names_order: Vector[String]
      ): DataFrame = {
    
    // Have sorted SNP names, an RDD where SNP -> values in order matching order of sample_names_order
    val sorted_SNP_names = inputRDD.keys.collect.sorted
    
    val schema = createSchema(sorted_SNP_names)
    /* 
     * NOTE: This involves an iterative loop with a side effect (reassignment to a true variable)
     */
    
    // Create the data frame that we will add the rows to
    var rowRDD = session.sqlContext.createDataFrame(session.sparkContext.emptyRDD[Row], schema)
    
    // For each sample name
    for (i <- 0 until sample_names_order.length) {
      val new_row = createRow(sample_names_order, i, sorted_SNP_names, inputRDD)      
      // before adding the row to the dataframe, it needs to be contained within a dataframe
      val rowDF = session.sqlContext.createDataFrame(session.sparkContext.makeRDD(List(new_row)), schema)
      rowRDD = rowRDD.union(rowDF)
    }
    return rowRDD
  }
  
  private[this] def addPair(pair: (String, String), df:DataFrame): DataFrame = {
    val col1 = pair._1
    val col2 = pair._2
    
    val sqlTrans = new SQLTransformer().setStatement(
        "SELECT *, (" + col1 + " * " + col2 + ") AS " + col1 + "_" + col2 + " FROM __THIS__" 
    )
    // Return the input data frame with the new column added
    return sqlTrans.transform(df)
  }
    
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
    // Add the first pair in the pair list to the dataframe
    val updated_df = addPair(input_pairs(0), input_df)
    
    // If that was the last pair to be added, return the final dataframe
    if (input_pairs.length == 1) {
      return updated_df
    }
    // If not, remove the pair that was just added from the pair list and call this again on the updated
    //   inputs
    else {
      val updated_pairs = input_pairs.slice(1, input_pairs.length)
      addPairwiseCombinations(updated_df, updated_pairs)      
    }
  }

  def performRegression(features: Array[String], df: DataFrame, label_col: String): RegressionOutput = {
        
    val features_name = "features(" + features.mkString(",") + ")"
    
    val assembler = new VectorAssembler().setInputCols(features).setOutputCol(features_name)
    
    val output = assembler.transform(df)
    
    output.show()
    
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
    
  def mapFunction(collections: StepCollections,
                  full_df: DataFrame,
                  phenotype: String
                 ): ParSeq[RegressionOutput] = {
    full_df.show()
   // In this implementation, the function is mapped to a collection on the
   //   driver node in a parallel fashion.
   println("collections not added size: " + collections.not_added.size)
   val reg_outputs = collections.not_added.toSeq.par.map(x => {
     // New value always added to the end of the features collection
      val features = collections.added_prev.toArray :+ x
      performRegression(features, full_df, phenotype)
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
  
  @tailrec
  def performSteps(spark: SparkSession,
                   df: DataFrame,
                   phenotype: String,
                   collections: StepCollections,
                   prev_best_model: RegressionOutput = null
                   ): RegressionOutput = {
    /*
     *  Step 1: find the best regression for those SNPs still under consideration
     */
    // Map generates all of the regression outputs, and reduce finds the best one
    val bestRegression = reduceFunction(mapFunction(collections, df, phenotype))
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
        // No more terms that could be added. Return the current best model
        return bestRegression
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
    val spark = SparkSession.builder.master("local").appName("Epistasis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
 
    /*
     *  Parse the input files
     */
    val SNPdata = parseInputFileWithSampleNames(SNP_file)
    val phenoData = parseInputFileWithSampleNames(pheno_file)
    
    /* Original SNPs
     * This is the same data used in the scala key-value association defined in the broadSNP variable,
     *   only now these are distributed across the cluster, i.e., it is a Spark key-value association, not
     *   the simpler scala map
     */
    val originalSNPs: rdd.RDD[(String, Vector[Double])] = spark.sparkContext.parallelize(SNPdata._1)
    val phenotypes = spark.sparkContext.parallelize(phenoData._1)
    
    val snp_df = createDataFrame(spark, originalSNPs, SNPdata._2)
    val pheno_df = createDataFrame(spark, phenotypes, phenoData._2)
    
    val pairs = createPairwiseList(snp_df)
    val full_df = addPairwiseCombinations(snp_df, pairs)
    
    // Dataframe is made persistent
    val pheno_SNP_df = full_df.join(pheno_df, "Samples").persist()
    
    val phenotype = phenoData._1(0)._1
    
    val snp_names = SNPdata._1.map(x => x._1)
    
    val not_added_init = HashSet() ++ snp_names
        
    val initial_collection = new StepCollections(not_added = not_added_init)
        
    val mapTest = mapFunction(initial_collection, pheno_SNP_df, phenotype) 
    
    /*mapTest.toSeq.foreach(x => {
      for (i <- 0 until x.featureNames.length) {
        println(x.featureNames(i) + ",")
        println(x.model.summary.pValues(i))
      }
    })
    
    val redTest = reduceFunction(mapTest)
    redTest.featureNames.foreach(println)
    println(redTest.model.summary.pValues(0))
    
    */
    threshold = 0.05
    
    val stepsTest = performSteps(spark, pheno_SNP_df, phenotype, initial_collection, null)
    stepsTest.featureNames.foreach(println)
    
    
  }
}