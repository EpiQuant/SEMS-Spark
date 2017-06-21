package prototypes.rddprototype

import prototypes.dfprototype.Parser
import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.broadcast._

object RDDPrototype {
  
  /** Reads in a <delimiter> separated file, and returns a new 2D Vector of its contents */
  def readFile(filePath: String, delimiter: String): Table = {
    val buffSource = Source.fromFile(filePath)
    return new Table(buffSource.getLines.toVector.map(_.split(delimiter).toVector))
  }

  /** Create a non-redundant pairwise list of names from a vector of string inputs
   *  
   *  Non-redundant means that X_Y is the same as Y_X, and we only create one
   *    of the two with this function
   *  */
  def createPairwiseList(columnNames: Vector[String]): IndexedSeq[(String, String)] = {
         
    // Creates a list of all pairwise combinations of each column (names only, does not
    //   actually compute the new values: that will be done per partition)
    for (i <- 0 until columnNames.length; j <- i + 1 until columnNames.length) yield {
      (columnNames(i), columnNames(j))
    }
  }
  
  /*
   * 1. Broadcast the original table throughout the cluster
   * 2. Create and distribute a list of columnName pairs for the SNP table
   * 3. On each Executor, create the SNP pairs for the columnName pairs on that Executor
   * 4. Perform all of the regressions in a Map and Reduce style
   */
    
  def createPairwiseColumn(pair: (String, String),
                           broadMap: Broadcast[Map[String, Vector[Double]]]
                          ): (String, Vector[Double]) = {
    val combinedName = pair._1 + "_" + pair._2
    val firstVals = broadMap.value(pair._1)
    val secondVals = broadMap.value(pair._2)
    val newVals = for (i <- 0 until firstVals.size) yield firstVals(i) * secondVals(i)
    (combinedName, newVals.toVector)
  }
  
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder.master("local[2]").appName("Epistasis").getOrCreate()
    //val spark = SparkSession.builder.appName("DataFramePrototype").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    val snpTable = readFile("/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/1106_Markers_NAM_Kernel_Color_Families_Only_for_R_10SNPs.txt",
                            "\t"
                           )
                           .deleteColumns(1,2,3,4)
                           .transpose
    
    snpTable.printTable                       
    
    val orderedSampleNames = snpTable.selectColumn(0).drop(1).map(_.toString)

    // The phenotype table's rows will be sorted according to those found in the SNP table
    val phenoTable = readFile("/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt",
                              "\t"
                             )
                             .sortRowsByNameList(orderedSampleNames)
    phenoTable.printTable
        
    val pairwiseCombinations = createPairwiseList(snpTable.getColumnNames)
        
    // Broadcast original SNP_Phenotype map
    val broadSnpTable = spark.sparkContext.broadcast(snpTable.createColumnMap)

    // Spread pairwise list across cluster
    val pairRDD = spark.sparkContext.parallelize(pairwiseCombinations)
    
    // Parallelize the original table into an RDD
    val singleSnpRDD = spark.sparkContext.parallelize(snpTable.createColumnList)
     
    // Create the pairwise combinations across the cluster
    val pairedSnpRDD = pairRDD.map(createPairwiseColumn(_, broadSnpTable))
   
    val fullSnpRDD = singleSnpRDD ++ (pairedSnpRDD).persist()
    
    fullSnpRDD
  }
  
  
  
}