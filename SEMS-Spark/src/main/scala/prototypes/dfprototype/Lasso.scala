

import scopt.OptionParser
import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec
import org.apache.spark.ml.regression._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Interaction
import org.apache.spark.ml.feature.VectorSlicer
import scala.collection.mutable.HashSet
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.ElementwiseProduct
import scala.collection.mutable.BitSet
import scala.Option
import prototypes.dfprototype.Parser
import org.apache.spark.ml.linalg.{ DenseVector, DenseMatrix, Vector, Vectors}
import breeze.linalg.{ *, DenseMatrix => BDM }
import org.apache.spark.mllib.linalg.BLAS
import scala.annotation.tailrec
import org.apache.spark.rdd.RDD



import org.apache.spark.ml.feature.SQLTransformer
import scala.reflect.runtime.universe._

class entry {
  val predictor: String = null
  val coef = 0.0
  val tval = 0.0
  val pval = 0.0
}


object Lasso {
   // val mySites: List[AdditiveSite] = 0
   // val myModel: List[ModelEffect] = null
    case class Params(
      snpFile: String = null,
      phenotypeFile: String = null,
      output: String = null,
      snpRmCols: Array[Int] = null,
      pheRmCols: Array[Int] = null,
      t1: Boolean = false, 
      t2: Boolean = false,
      significance: Double = 0.05
      ) 
    
  val defaultParams = Params()
  
    case class RegressData(
      val Untested: Array[String] = null,
      //val untested: BitSet,
      //val skipped: BitSet = BitSet(),
      //var Included: Vector[String] = null,
      //var Skipped: Vector[String] = null,
      var df: DataFrame = null
    )
 
  
/*  def filterColumn(arr: Vector[String], cols: Array[Int]): Vector[String] = {
    val zipped = arr.zipWithIndex
    val filtered = zipped.filterNot(x => cols.contains(x._2))
    return filtered.map(x => x._1)
  }*/
  
  def main(args: Array[String]){

Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)

run(args)
  /*    val parser = new OptionParser[Params]("epiquant") {
      head("EPIQUANT: an statsistical analysis for GWAS data in SPARK/scala.")
      opt[String]('s', "snpFile")
        .valueName("<snp file>")
        .text(s"input path to file with SNP values for individuals")
        .required
        .action((x, c) => c.copy(snpFile = x))
      opt[String]('p', "pheFile")
        .valueName("<phenotype file>")
        .text(s"input path to file specifying the phenotypic data for individuals")
        .required
        .action((x, c) => c.copy(phenotypeFile = x))
      opt[String]('o', "outFile")
        .valueName("<output file>")
        .text(s"path to output file")
        .action((x, c) => c.copy(output = x))
        
      opt[String]("cols_1").abbr("c1")
        .valueName("\"1 2 3...\"")
        .text(s"Unwanted columns in snpFile, zero indexed, separated by space.")
        .action((x, c) => c.copy(snpRmCols = x.split(" ").map(y => y.toInt))) 
      opt[String]("cols_2").abbr("c2")
        .valueName("\"1 2 3...\"")
        .text(s"Unwanted columns in pheFile, zero indexed, separated by space.")
        .action((x, c) => c.copy(pheRmCols = x.split(" ").map(y => y.toInt)))
        
      opt[Unit]("snpFile transpose")
        .abbr("t1")
        .text(s"whether the snpFile data matrix needs transpose. Default: false")
        .action((x, c) => c.copy(t1 = true))
      opt[Unit]("pheFile transpose")
        .abbr("t2")
        .text(s"whether the pheFile data matrix needs transpose. Default: false")
        .action( (x, c) => c.copy(t2 = true) )
      opt[Double]('a', "alpha")
        .text(s"significance level: a threshold value for p-value test. " +
            s"Default: 0.05")
        .action((x, c) => c.copy(significance = x))
      checkConfig { params =>
        if (params.significance < 0 || params.significance >= 1) {
          failure(s"fracTest ${params.significance} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
     }
      
      
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
    
    */
    
  }
  
  def argMin(array: Seq[Double]): Int = {
    val result = array.foldLeft(-1,Double.MaxValue,0) {
        case ((minIndex, minValue, currentIndex), currentValue) =>
            if(currentValue < minValue) (currentIndex,currentValue,currentIndex+1)
            else (minIndex,minValue,currentIndex+1)
        }
    result._1
}
  
  def run(args: Array[String]){
     val spark = SparkSession
      .builder()
      .appName("Lasso")
      .master("local")
      .getOrCreate()
  
    /* val snp = CreateDataframe(spark, params.snpFile, params.t1, params.snpRmCols)
     
     // want to be able to include certain snps for sure.
     
     //make pairwise //
      val pairwiseSnp = makePairwise(snp.df, snp.Untested.length, snp.Untested).cache()
      pairwiseSnp.show(truncate=false)
      
     val phe = CreateDataframe(spark, params.phenotypeFile, params.t2, params.pheRmCols)
     val onephe = phe.df.select("Samples", "Trait1")
      
      val df = LassoRegression("Samples", pairwiseSnp, onephe, "Trait1", 0.0)
      val df1 = LassoRegression("Samples", pairwiseSnp, onephe, "Trait1", 0.00001)
      val df2 = LassoRegression("Samples", pairwiseSnp, onephe, "Trait1", 0.001)
      val df3 = LassoRegression("Samples", pairwiseSnp, onephe, "Trait1", 0.01)
      spark.stop()*/
      
      val denseRDD = Parser.parser(spark, args)
      val initialWeights = 0
        

      
    //  val a  = optimize(denseRDD, )
      
  }
  
  /*def optimize(data: RDD[(DenseVector, DenseMatrix)], initialWeights: Vector, 
      xy: Array[Double], lambdas: Array[Double], alpha: Double, lamShrnk: Double, 
      maxIter: Int, tol: Double, numFeatures: Int, numRows: Long): List[(Double, Vector)] = {
    
    List[(0.0, new Vector(0,0,0))]
    
  }*/
  
  def makePairwise(df: DataFrame, n: Int, features: Array[String]): DataFrame = {
 
    val assembler = new VectorAssembler().
      setInputCols(features).
      setOutputCol("vec1")
      
    val assembled1 = assembler.transform(df)
    val assembled2 = assembler.setOutputCol("vec2").transform(assembled1)

    val interaction = new Interaction()
      .setInputCols(Array("vec1", "vec2"))
      .setOutputCol("interactedCol")
   
    val interacted = interaction.transform(assembled2)
   // var dataframe: DataFrame  = interacted
    
    /*for (i <- arr) {
      slicer.setOutputCol("Pairwise_" + i.toString())
      .setIndices(Array(i))
      dataframe = slicer.transform(dataframe)
    }*/
   
    val arr = UpMIndex(n) 
    val slicer = new VectorSlicer().setInputCol("interactedCol").setIndices(arr).setOutputCol("Pairwise")
    val pairwise = slicer.transform(interacted)

    assembler.setInputCols(Array("vec1", "Pairwise")).setOutputCol("features")
    assembler.transform(pairwise)
  }
  
 /* private def normalizeDataSet(dataset: DataFrame): (RDD[(Double, Vector)], StandardScalerModel) = {
    val instances = extractLabeledPoints(dataset).map {
      case LabeledPoint(label: Double, features: Vector) => Vectors.dense(label +: features.toArray)
    }

    val scalerModel = new StandardScaler(withMean = true, withStd = true)
      .fit(instances)

    val normalizedInstances = scalerModel
      .transform(instances)
      .map(row => (row.toArray.take(1)(0), Vectors.dense(row.toArray.drop(1))))

    (normalizedInstances, scalerModel)
  }*/
  
  private def fit(dataset: DataFrame){
    // val (normalizedInstances, scalerModel) = normalizeDataSet(dataset)
  }
  
 def UpMIndex(n: Int): Array[Int] ={
  var count = 0
  var c2 = 1
  var ceng = 0
  val arr = for (i <- 0 until (n*(n+1)/2)-n)  yield{
    if(c2 >= n-ceng)  {
      count += (3+ceng)
      ceng+=1
      c2=2
      count
    }
    else {
      count= count +1
      c2+=1
      count
    }
  } 
  arr.toArray
  
}

  def LassoRegression(Trait:String, snp: DataFrame, onephe: DataFrame, LabelCol: String, regParam: Double): DataFrame = {
     
      val df = snp.join(onephe, Trait)
      //df.show(truncate=false)
      
    // Summarize the model over the training set and print out some metrics
      
     val lir = new LinearRegression().
     setLabelCol(LabelCol).
     setFeaturesCol("features").
     setElasticNetParam(1.0)
     .setRegParam(regParam)

     
     val startTime = System.nanoTime()
     val lrModel = lir.fit(df)
     val elapsedTime = (System.nanoTime() - startTime) / 1e9
     val coeffs = lrModel.coefficients
    // val pvals = lrModel.summary.pValues
     
     println(s"Training time: $elapsedTime seconds")
      
     println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
     
    // val index = argMin(pvals)
    // println("argMin: " + index + ", minpval: " + pvals(index))
     df
  }
  
  def CreateDataframe(spark: SparkSession, file: String, t: Boolean, rmCols: Array[Int]): RegressData ={


val rdd = spark.sparkContext.parallelize(Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9)))
// Split the matrix into one number per line.
val byColumnAndRow = rdd.zipWithIndex.flatMap {
  case (row, rowIndex) => row.zipWithIndex.map {
    case (number, columnIndex) => columnIndex -> (rowIndex, number)
  }
}
// Build up the transposed matrix. Group and sort by column index first.
val byColumn = byColumnAndRow.groupByKey.sortByKey().values
// Then sort by row index.
val transposed = byColumn.map {
  indexedRow => indexedRow.toSeq.sortBy(_._1).map(_._2)
}
    val fileRDD = spark.sparkContext.textFile(file); // Source.fromFile(file).getLines().toVector
    // RDD[ String ]
     
    /* filter out the unnecessary columns as supplied in the main arguments */
    val splitRDD = if (rmCols != null) {
          fileRDD.map{
            line => line.split("\t").zipWithIndex
            .filterNot(x => rmCols.contains(x._2)).map(x => x._1)
          }
        } else fileRDD.map(line => line.split("\t"))
    // RDD[ Array[ String ]]
    
    val finalData =
    /* transpose the data if necessary */
    if(t == true){
      val byColumnAndRow = splitRDD.zipWithIndex.flatMap {
        case(row, rowIndex) => row.zipWithIndex.map{
          case(string, columnIndex) => columnIndex -> (rowIndex, string)
        }
      }
      val byColumn = byColumnAndRow.groupByKey.sortByKey().values
      byColumn.map {
        indexedRow => indexedRow.toArray.sortBy(_._1).map(_._2)
      }
    } else {
      splitRDD
    }
    
   

    /* filter out header row */
    val header = finalData.first()
    val filtered = finalData.mapPartitionsWithIndex((idx, iter) => if (idx ==0 ) iter.drop(1) else iter)  
    //filter(row => row.sameElements(header))
    
   
    
    /* turn filtered data into Rows, where in each Row 
     * the first element is a String (the distinct identifier for individuals) 
     * followed by Doubles (actual data for each SNP)
     */
    val rowRDD = filtered.map{
          attributes => Row.fromSeq(attributes(0) 
             +: attributes.slice(1, attributes.length).map(x=>x.toDouble))
          }
     
    /* build schema: the first column is of StringType and the rest of columns are DoubleType
     * corresponding to the RDD of Rows int the previous step
     */
    //val schema = StructType((0 to finalData(0).length-1).toArray  
      //.map(fieldName => StructField(fieldName.toString(), if(fieldName == 0) StringType else DoubleType, nullable = false)))

    val schema = StructType(header
      .map(fieldName => StructField(fieldName, if(fieldName == header(0)) StringType else DoubleType, nullable = false)))
   
    RegressData(df=spark.createDataFrame(rowRDD, schema), Untested = header.drop(1))
  }
}


/*val coder = (arg: Vector[Double]) => {
    arg.filterNot( x => arg.indexOf(x) % arg.length == 0)
  }
  val coder1: (Double => Double) = (arg: Double) => arg+1.0
val sqlfunc = udf(coder1)
val sqlfunc2 = udf(coder)

val gg = col("id1")

val c = interacted.withColumn("Code", sqlfunc(col("id1")))
c.show(truncate = false)
*/

/*Iterator.continually{   if(count>= n-ceng)  {
    count+=(2+ceng)
    ceng+=1
  }; count }.takeWhile(count => count < (n*n))*/
  

/*val a = for (r <- 0 until n) yield { // each row
  for (c <- r until n) yield {
    val i = (n * r) + c - ((r * (r+1)) / 2); // corrected from earlier post
    i
  }
}
  val g =a.flatMap { x => x }
 a.foreach(x => x.foreach(println))
 g.foreach(println)*/

// def coder(myAmt:Double):String = {
  //if (myAmt > 100) "Little"
 // else "Big"
 //}
 