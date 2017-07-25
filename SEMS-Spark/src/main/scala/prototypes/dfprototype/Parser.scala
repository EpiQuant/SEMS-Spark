package prototypes.dfprototype

import scopt.OptionParser
import scala.io.Source


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

//import prototypes.dfprototype.RowPartionedTransformer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe
import scala.reflect._
import org.apache.spark.mllib.linalg.{ DenseVector, DenseMatrix, Vector, Vectors}

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV,  DenseMatrix => BDM , Matrix => BM}

//import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD



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
   /* case class Params(
      input: String = null,
      dataFormat: String = "libsvm",
      regParam: Double = 0.0,
      unwantedColumns: String = ""//,
      ) extends AbstractParams[Params]*/
    
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
  
  def createParser() = {
       val parser = new OptionParser[Params]("epiquant") {
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
     parser 
  }

   
  def parse(spark: SparkSession, args: Array[String]) = {
      val snp_f = args(0)
      val phe_f = args(1)

//      spark.sparkContext.addFile(snp_f )//"resources/sampleSNP.txt")
//      spark.sparkContext.addFile(phe_f) //"resources/Simulated.Data.100.Reps.Herit.0.5_Phe1.txt")

// val snp_file = SparkFiles.get(snp_f)
//println("I am hererererere!")
//println(snp_file)
// val phe_file = SparkFiles.get(phe_f)
//println(phe_file)


//val hdfs = FileSystem.get(new URI("hdfs://aforge015-ib-priv.local:8020/"), new Configuration())
//val snp_path = new Path("user/xnie8/resources/1106_Markers_NAM_Kernel_Color_Families_Only_for_R.txt")//args(0))
//val phe_path = new Path("user/xnie8/resources/Simulated.Data.100.Reps.Herit.0.5_Phe1.txt")

//val snp_stream = hdfs.open(snp_path)
//def readSNPs = Stream.cons(snp_stream.readLine, Stream.continually(snp_stream.readLine))
//val data = readSNPs.takeWhile(_!= null).map(_.split("\t")).toArray

      val data = Source.fromFile(snp_f).getLines().map(_.split("\t")).toArray
      val samples = data(0).drop(5)
      val snp_names = data.drop(1).map(x => x(0))

//val phe_stream = hdfs.open(phe_path)
//def readPhe = Stream.cons(phe_stream.readLine, Stream.continually(phe_stream.readLine))
//val phefile = readPhe.takeWhile(_!= null).map(_.split("\t")).toArray

      val phefile = Source.fromFile(phe_f).getLines().map(_.split("\t")).toArray
      val phe_samples = phefile.drop(1).map(x => x(0))
      val phe_names = phefile(0).drop(1)
      //val phe_vales = phefile.drop(1).flatMap(x => x.drop(1).map(_.toDouble))
      val phe_vales_array = phefile.drop(1).map{
        x => x.drop(1).map(_.toDouble)
      }

     val snp_darray = data.drop(1).map(x => x.drop(5).map(_.toDouble)).transpose   

     val stick = for (i <- 0 to snp_darray.length-1) yield {
       (snp_darray(i),phe_vales_array(i))
     }
      
     val rdd = spark.sparkContext.parallelize(stick)
     val part = rdd.getNumPartitions

     //construct the matrix snp_matrix(i)
     val values = rdd.mapPartitionsWithIndex{
       (indx, x) => {
         val snp_1 : Array[Array[Double]] = new Array[Array[Double]](samples.length/part+1)
         val phe_2 : Array[Array[Double]] = new Array[Array[Double]](samples.length/part+1)
         val n = snp_names.length 
	       var num_rows = 0
         
      	 while(x.hasNext){
      	     val temp = x.next
             snp_1(num_rows) = temp._1
             phe_2(num_rows) = temp._2
      	     num_rows+=1
      	  }
         
          //TODO: unnecessary reconstruction of phe_matrix. see if can construct in previous loop.
         val snp_matrix = BDM.zeros[Double](num_rows, (n*(n+1)/2))
         val phe_matrix = BDM.zeros[Double](num_rows, phe_names.length)
        
         for (k <- 0 until num_rows) {
    		    var c = n
    		    for (i <- 0 until n-1) {
               	for (j <- i+1 until n) {	
    			          snp_matrix(k, c) = snp_1(k)(i) * snp_1(k)(j)
    			          c+=1
    		        }
           	}
        		for (i <- 0 until phe_names.length) {
        			phe_matrix(k, i) = phe_2(k)(i)
        		}
        }
         
        Iterator((snp_matrix, phe_matrix, num_rows))
       }   
     }.persist(storage.StorageLevel.MEMORY_AND_DISK)
     
     (values, snp_names, phe_names, samples) 

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

  }//end CreateDataFrame
  
  
}//end class

 
