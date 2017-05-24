package Test

import scopt.OptionParser
import scala.io.Source
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.annotation.tailrec
import org.apache.spark.ml.regression._
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.feature.SQLTransformer
import scala.reflect.runtime.universe._

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
      
// For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    val data: Vector[String] = Source.fromFile("resources/sampleSNP.txt").getLines().toVector
    val zdata: Vector[Vector[String]] = data.map(x => x.split("\t").toVector)
    /* store the header row/column*/
    var header_column = zdata.map(x => x(0))
    var header_row = zdata(0)
    
    /* Get rid of the first row */
    val zdata2 = zdata.filter(x => x!=header_row)          //filter out header  
    
    /* Get rid of first column and Change to Double */
    val ddata = for (i <- 0 until zdata2.length) yield {
      filterColumn(zdata2(i), Array(0), 1).map(x => StrToDouble(x).getOrElse(i+1.0))
    }
    
    /* Delete first entry in header row/column to match ddata */
    header_row = header_row.filter(x => x != header_row(0))
    header_column = header_column.filter(x => x != header_column(0))
    
    var fdata = ddata
    /* Command-Line Dependent */
    val temp = args(0).split(" ")  
    val vcols = for (string <- temp) yield {
      string.toInt-1
    }
    /* filter out the unecessary columns as supplied in the main arguments */
    val filtered = for (i <- 0 until ddata.length) yield {
      filterColumn(ddata(i).toVector, vcols)
    }
    header_row = filterColumn(header_row, vcols, 0)
    fdata = filtered

    /* transpose the data if necessary */
    val transposed = fdata.transpose
    println("Transpose: ")
    fdata = transposed.map(x => x.toVector)
 
    val rowRDD =   //turn data into Rows
      spark.sparkContext.parallelize(fdata).map(attributes => Row.fromSeq(attributes))
    
      
    //val schema = StructType(header.split("\t")     //build schema
    // .map(fieldName => StructField(fieldName, DoubleType, nullable = false)))
    val schema = StructType(header_column     //build schema
      .map(fieldName => StructField(fieldName, DoubleType, nullable = false)))

    val dataframe = spark.createDataFrame(rowRDD, schema) //create Dataframe
    //dataframe.createOrReplaceTempView("Epitasis")
    //dataframe.show()

    val broadcastVar = spark.sparkContext.broadcast(dataframe)
    val training = spark.read.format("libsvm")
  .load("data/mllib/sample_linear_regression_data.txt")

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.3)
  .setElasticNetParam(0.8)

// Fit the model
val lrModel = lr.fit(training)

    
    
  }
}