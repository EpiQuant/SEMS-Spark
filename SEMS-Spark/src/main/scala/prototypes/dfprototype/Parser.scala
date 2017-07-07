package prototypes.dfprototype

import scopt.OptionParser
import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe
import scala.reflect._

abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}


object Parser {
    case class Params(
      input: String = null,
      dataFormat: String = "libsvm",
      regParam: Double = 0.0,
      unwantedColumns: String = ""//,
      ) extends AbstractParams[Params]
    
  val defaultParams = Params()
  
  def Parsing(): Unit = {
     val parser = new OptionParser[Params]("LinearRegressionExample") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[String]("Unwanted columns")
        .text(s"Unwanted columns, zero indexed.")
        .action((x, c) => c.copy(unwantedColumns = x))
      opt[String]("dataFormat")
        .text("data format: libsvm (default)")
        .action((x, c) => c.copy(dataFormat = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      //arg[String]("<>")
    }
  }
  
  def StrToDouble(str: String): Option[Double] = {
    try {
        Some(str.toDouble)
    } catch {
        case e: NumberFormatException => None
    }
  }
  
  def filterColumn(arr: Vector[Double], cols: Array[Int]): Vector[Double] = {
    val zipped = arr.zipWithIndex
    val filtered = zipped.filterNot(x => cols.contains(x._2))
    return filtered.map(x => x._1)
  }
  
  def filterColumn(arr: Vector[String], cols: Array[Int], int: Int): Vector[String] = {
    val zipped = arr.zipWithIndex
    val filtered = zipped.filterNot(x => cols.contains(x._2))
    return filtered.map(x => x._1)
  }
  def main(args: Array[String]){
      val spark = SparkSession
      .builder()
      .appName("Parser")
      .master("local")
      .getOrCreate()
      
     val temp = args(0).split(" ")  
     val vcols = for (string <- temp) yield {
      string.toInt-1
     }
      
      val snp = CreateDataframe(spark, "resources/sampleSNP.txt", true, vcols)
       snp.createOrReplaceTempView("Epitasis")
      snp.show()
     
      val phe = CreateDataframe(spark, "resources/sampleData.txt", false, null)
      phe.show()
      
  }
  
  def CreateDataframe_obsolete(spark: SparkSession, file: String, t: Boolean, rmCols: Array[Int]): DataFrame = {

    val data: Vector[String] = Source.fromFile(file).getLines().toVector
    val splitLines: Vector[Vector[String]] = data.map(x => x.split("\t").toVector)
    
    var finalData: IndexedSeq[IndexedSeq[Any]] = splitLines
    
    /* filter out the unnecessary columns as supplied in the main arguments */
    if(rmCols != null){
      val filtered = for (i <- 0 until splitLines.length) yield {
        filterColumn(splitLines(i).toVector, rmCols, 0)
      }
      finalData = filtered
    }
    /* transpose the data if necessary */
    if(t == true){
      val transposed = finalData.transpose
      finalData = transposed
    }
    
    /* filter out header row */
    val filtered = finalData.slice(1, finalData.length)
    
    /* turn filtered data into Rows, where in each Row 
     * the first element is a String (the distinct identifier for individuals) 
     * followed by Doubles (actual data for each SNP)
     */
    val rowRDD = spark.sparkContext.parallelize(filtered)
      .map({attributes => 
          Row.fromSeq(attributes(0).toString() 
          +: attributes.slice(1, attributes.length).map(x=>x.toString().toDouble))
          })
            
    /* build schema: the first column is of StringType and the rest of columns are DoubleType
     * corresponding to the RDD of Rows int the previous step
     */
    val schema = StructType(finalData(0)     
      .map(fieldName => StructField(fieldName.toString(), if(fieldName == finalData(0)(0)) StringType else DoubleType, nullable = false)))
    
    /* create DataFrame from the RDD of Rows and schema */  
    spark.createDataFrame(rowRDD, schema)
  }





 def CreateDataframe(spark: SparkSession, file: String, t: Boolean, rmCols: Array[Int]): DataFrame ={

    val fileRDD = spark.sparkContext.textFile(file); 

    /* filter out the unnecessary columns as supplied in the main arguments */

    val splitRDD = if (rmCols != null) {
          fileRDD.map{
            line => line.split("\t").zipWithIndex
            .filterNot(x => rmCols.contains(x._2)).map(x => x._1)
          }
        } else fileRDD.map(line => line.split("\t"))
   
   /* transpose the data if necessary */
    val finalData = 
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
    
val schema = StructType(header
      .map(fieldName => StructField(fieldName, if(fieldName == header(0)) StringType else DoubleType, nullable = false)))
  
    spark.createDataFrame(rowRDD, schema)

  }


}
