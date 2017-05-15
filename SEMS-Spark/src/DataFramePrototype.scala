import scala.io.Source
import scala.annotation.tailrec
import org.apache.spark.ml.regression._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.feature.SQLTransformer
import org.apache.spark.ml.regression._

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
  
  def createSchema(sorted_snp_names: Array[String]): StructType = {
    
    val SNP_headers = sorted_snp_names.map(name => (StructField(name, DoubleType, nullable = true))).toList
    
    // prepends the samples column header to the snp headers list
    val schema_list = StructField("Samples", StringType, nullable = true) :: SNP_headers;
    
    val schema = StructType(schema_list)
    return schema
  }
  
  def createRow(sample_names_order: Vector[String],
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
    println(schema)
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
  
  def addPair(pair: (String, String), df:DataFrame): DataFrame = {
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
    
  def performRegression(features: List[String], df: DataFrame, label_col: String): LinearRegressionModel = {
    val features_name = "features(" + features.mkString(",") + ")"

    val assembler = new VectorAssembler().setInputCols(features.toArray).setOutputCol(features_name)
        
    val output = assembler.transform(df)
    
    val lr = new LinearRegression().setLabelCol(label_col).setFeaturesCol(features_name)
    val reg = lr.fit(output)
    return reg
  }
  
  def performSteps(spark: SparkSession, added_prev: List[String], possible: Array[String],
                   skipped: List[String], df: DataFrame, phenotype: String,
                   previous_regression: LinearRegressionModel
                  ): LinearRegressionModel = {
    /* 
     * added_prev: the SNPs that are already included in the model
     * possible: the SNPs that may still be added to the model
     * skipped: SNPs that were removed in the previous step will not be considered in this iteration
     * df: The data frame that contains the SNPs and phenotypes
     */ 
    val best_regression = spark.sparkContext.parallelize(possible).map( x => {
      val features = x :: added_prev
      performRegression(features, df, phenotype)
      // The SNP under testing will have its p-value in the zeroth position, get the minimum
    }).reduce((y, z) => if (y.summary.pValues(0) <= z.summary.pValues(0)) y else z)
    
    // Now that the regressions are finished for this iteration, add the skipped back into the consideration
    // category for next time
   // val new_possible = 
    
    val reg_pValues = best_regression.summary.pValues
    
    if (reg_pValues(0) >= threshold) {
      // The model building is completed
      return previous_regression
    }
    else {
      // First, include it in the model and remove it from the 'possible' category
      val new_model =  :: model
      
      
      /*
       * Check whether any of the previously included terms are no longer significant
       * If so, remove them from the model and leave them out of the next iteration (but bring them back
       * into consideration later)
       */ 
      // Ignore the first and the last p-values: the first is the one that was just added, and the last is
      // the intercept: there should be a correspondence between the "added_previously" array and the p-values
      // being looped through here
      for (i <- 1 until (reg_pValues.length - 1) ) {
        if (reg_pValues(i) >= threshold) {
          // remove from model, and put it in the 'skipped' list
        }
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
        
    val pheno_SNP_df = full_df.join(pheno_df, "Samples").persist()
    pheno_SNP_df.show()
    
    val reg = performRegression(Array("SNP1_SNP3"), pheno_SNP_df, "PH1")
    reg.summary.objectiveHistory.foreach(println)
    reg.summary.pValues.foreach(println)
    println(reg.summary.featuresCol)
     
  }
  
  
  
}