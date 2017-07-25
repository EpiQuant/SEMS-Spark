package prototypes.dfprototype

import scopt.OptionParser
import scala.io.Source
import scala.util.control.Breaks._

//import prototypes.dfprototype.RowPartionedTransformer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression._
import scala.reflect.runtime.universe._
import scala.reflect.runtime.universe
import scala.reflect._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.mllib.linalg.{ DenseVector, DenseMatrix, Vector, Vectors}
//import org.apache.spark.mllib.linalg.{ DenseMatrix, DenseVector}
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV,  DenseMatrix => BDM , Matrix => BM}
import org.apache.spark.mllib.linalg.CholeskyDecomposition
import prototypes.dfprototype.BLAS
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix, RowMatrix}
import breeze.linalg._
import breeze.linalg.functions._
import breeze.optimize.proximal.QuadraticMinimizer
import breeze.numerics._

//import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StandardScaler


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
  
  def main(args: Array[String]){
   // Logger.getLogger("org").setLevel(Level.OFF)
  // Logger.getLogger("akka").setLevel(Level.OFF)
      val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Parser")
      .master("local")
      .getOrCreate()
    val a = parser(spark, args)
      
  }
   // solve Ax = b, for x, where A = choleskyMatrix * choleskyMatrix.t
   // choleskyMatrix should be lower triangular
   def solve( choleskyMatrix: BDM[Double], b: BDV[Double], x: BDV[Double] )  = {
      val C = choleskyMatrix
      val size = C.rows
      if( C.rows != C.cols ) {
          // throw exception or something
      }
      if( b.length != size ) {
          // throw exception or something
      }
      // first we solve C * y = b
      // (then we will solve C.t * x = y)
      val y = BDV.zeros[Double](size)
      // now we just work our way down from the top of the lower triangular matrix
      for( i <- 0 until size ) {
         var sum = 0.0
         for( j <- 0 until i ) {
            sum += C(i,j) * y(j)
         }
         y(i) = ( b(i) - sum ) / C(i,i)
      }
      // now calculate x
      //val x = BDV.zeros[Double](size)
      val Ct = C.t
      // work up from bottom this time
      for( i <- size -1 to 0 by -1 ) {
         var sum = 0.0
         for( j <- i + 1 until size ) {
            sum += Ct(i,j) * x(j)
         }
         x(i) = ( y(i) - sum ) / Ct(i,i)
      }
      
   }
  def parser(spark: SparkSession, args: Array[String]) = {
       // val temp = args(0).split(" ")  


      spark.sparkContext.addFile("resources/sampleSNP.txt")
      spark.sparkContext.addFile("resources/Simulated.Data.100.Reps.Herit.0.5_Phe1.txt")
      val data = Source.fromFile("resources/sampleSNP.txt").getLines().map(_.split("\t")).toArray
      val samples = data(0).drop(5)
      val snp_names = data.drop(1).map(x => x(0))
      var i = 0
      //val snp_matrix = new DenseMatrix(snp_names.length, samples.length, data.drop(1).flatMap(x => x.drop(5).map(_.toDouble)))
      /*val values = spark.sparkContext.parallelize{
        data.drop(1).map{x => 
          val ret = IndexedRow(i, Vectors.dense(x.drop(5).map(_.toDouble)))
          i+=1
          ret
        }
      }*/
      
      val phefile = Source.fromFile("resources/Simulated.Data.100.Reps.Herit.0.5_Phe1.txt").getLines().map(_.split("\t")).toArray
      val phe_samples = phefile.drop(1).map(x => x(0))
      val phe_names = phefile(0).drop(1)
      //val phe_vales = phefile.drop(1).flatMap(x => x.drop(1).map(_.toDouble))
      val phe_vales_array = phefile.drop(1).map{
        x => x.drop(1).map(_.toDouble)
      }
      
      println(phe_samples.length)
      println(phe_names.length)
      
      //val phevals = new DenseMatrix(phe_samples.length, phe_names.length, phe_vales, true)
      //val colIter = phevals.colIter
      //val PH1 = colIter.next
      
      
     
     val snp_darray = data.drop(1).map(x => x.drop(5).map(_.toDouble)).transpose
     println("SNP: m", snp_darray.length, "n/D", snp_darray(0).length )
     val stick = for (i <- 0 to snp_darray.length-1) yield {
       snp_darray(i)++phe_vales_array(i)
     }
      println("stick: m", stick.length, "n/D", stick(0).length )
      
     val rdd = spark.sparkContext.parallelize(stick)
     val part = rdd.getNumPartitions
     println("partition", part)
      //TODO:
     //construct the matrix snp_matrix(i)
     val values = rdd.mapPartitions{
       x => {
          //TODO: val x_duplicate = x.duplicate
         var snp_1 : Array[Double] = Array()
         var phe_2 : Array[Double] = Array()
         var num_rows = 0
         while(x.hasNext){
           num_rows+=1
           snp_1 = snp_1 ++ x.next.slice(0, snp_names.length)
           phe_2 = phe_2 ++ x.next.slice(snp_names.length, snp_names.length+phe_names.length)
         }
         // println("SNP: m", snp_1.length, "n/D", snp_1(0).length )
         // println("PHE: m", phe_2.length, "n/D", phe_2(0).length , num_rows)
          
          //TODO: unnecessary passing of data n . see if can extract from BDM dimension
          println(snp_1.length)
          println(snp_1(0).length)
          println(num_rows)
         // println("paritition",idx,"num samples",num_rows)
          Iterator((BDM(snp_1:_*), BDM(phe_2:_*), num_rows))
       }
         
     }.cache()

     
     var rho = 1.0;
     val D = snp_names.length
     val maxIter = 100
     val lambda = 0.5
     val absTol = 1e-4
     val relTol = 1e-2

      var cholesky_A = values.mapPartitionsWithIndex{
        (index, iterator) => {
          var i =0
          println(i)
          var g = iterator.next
          while(iterator.hasNext){
            i+=1
            println(i) //make sure only 1 matrix per partition
          }
          val snp_matrix = g._1
          val phe_matrix = g._2
          val n = g._3 //number of samples in this partition
         //precompute Atb
          var rho = 1.0
          val skinny = (n >=D)
          //TODO compare with breeze matrix vector multiplication speed
          
          val Atb = phe_matrix(::, *).map(dv => (snp_matrix.t *dv))
         
          var L =  new BDM(1, 1, Array(0.0))
          if (skinny){
            //Compute the matrix A: A = chol(AtA + rho*I) 
            val AtA = snp_matrix.t * snp_matrix//new DenseMatrix(D, D, Array.fill[Double](D*D)(0))
            val rhoI = BDM.eye[Double](n)
            rhoI*=rho
            L = cholesky(AtA + rhoI)
          } else{
            //compute the matrix L : L = chol(I + 1/rho*AAt) 
            val AAt = snp_matrix * snp_matrix.t
            AAt *= 1/rho
            val eye = BDM.eye[Double](n)
            L = cholesky(AAt+eye)
          }
          
         val x = BDV.zeros[Double](D)
         
         val u = BDV.zeros[Double](D)
         //val y = BDV.zeros[Double](D)
         //val r = BDV.zeros[Double](D)
         val zprev = BDV.zeros[Double](D)
         //val zdiff = BDV.zeros[Double](D)
         //val q = BDV.zeros[Double](D)
         //val w = BDV.zeros[Double](D)
          
         
         //TODO find out a way to map Atb
          Iterator((L, Atb(::,0), n, x, u, zprev, snp_matrix))
        }
     } 
     
     val zprev = BDV.zeros[Double](D)
     val zdiff = BDV.zeros[Double](D)
     val z = BDV.zeros[Double](D)
     var iter = 0  
     var prires = 0.0
     var nxstack = 0.0
     var nystack = 0.0
     var bc_z = cholesky_A.sparkContext.broadcast(z)
     val N = 4 //num of partitions
     
     val startTime = System.nanoTime()
     while(iter < maxIter){
        cholesky_A = cholesky_A.mapPartitions{
          iterator => {
                      var i =0
          println(i)
          var g = iterator.next
          while(iterator.hasNext){
            i+=1
            println(i) //make sure only 1 matrix per partition
          }
          val G = iterator.next()
          
          val L =G._1
          val Atb = G._2
          val m = G._3
          val x = G._4
         // val z = G._5
          val z = bc_z.value
          val u = G._5
          val zprev = z.copy
          val A = G._7 //snp_matrix
          val skinny = m >= D
          
          // u-update: u = u + x - z
           u+=x
           u-=z
          // x-update: x = (A^T A + rho I) \ (A^T b + rho z - y) 
          val q = z.copy
          q:-=u
          q:*=rho
          q :+=Atb 
          
          if(skinny){
            solve( L, q, x )
          }else{
            //val Aq = BDV.zeros[Double](n) // new DenseVector(Array.fill[Double](N)(0))
            //QuadraticMinimizer.gemv(1, L, q, 0, Aq)
            val p = BDV.zeros[Double](m)
            val Aq = A*q
            solve(L, Aq, p)
            //BLAS.gemv(1, fromBreeze(A), fromBreeze(q), 0, Aq)
            //solve(A, asBreeze(Aq), p)
            //gsl_blas_dgemv(CblasTrans, 1, A, b, 0, Atb); // Atb = A^T b
            QuadraticMinimizer.gemv(1, A.t, p, 0, x)
            x:*= -1/(rho*rho)
            q:*= 1/rho
            x:+=q  
          }
          
          Iterator((L, Atb, m, x, u, zprev, A))
        }
          
      }//end MapPartition
          
        
        /*
		 		 * Message-passing: compute the global sum over all processors of the
		 		 * contents of w and t. Also, update z.
		 		 */
        
        val recv = cholesky_A.map{
          A =>{
              val x = A._4
              val z = bc_z.value
              val u = A._5
 
              val r = x-z
              val r_sq = r dot r
              val x_sq = x dot x
              val u_sq = (u dot u)/(rho*rho)
              
            Array(r_sq, x_sq, u_sq)
          }
        }.treeReduce((x, y)=> Array(x(0)+y(0), x(1)+y(1), x(2)+y(2)), 2)

        val new_z = cholesky_A.map{
          A => {
              val x = A._4
              val u = A._6
              x + u
          }
        }.treeReduce(_+_, 2)
        
        zprev := bc_z.value
        z := soft_threshold(new_z*(1.0/N), lambda/(N*rho))
        bc_z = cholesky_A.sparkContext.broadcast(z)
 
        
            prires  = sqrt(recv(0))  /* sqrt(sum ||r_i||_2^2) */
    		    nxstack = sqrt(recv(1))  /* sqrt(sum ||x_i||_2^2) */
     		    nystack = sqrt(recv(2))  /* sqrt(sum ||y_i||_2^2) */
     
        zdiff := z-zprev
 		   val dualres = sqrt(N)*rho * norm(zdiff)
 		   
 		   //primal and dual feasibility tolerance
 		   val eps_pri = sqrt(D*N)*absTol + relTol * max(nxstack, sqrt(N)*norm(z))
 		   val eps_dual = sqrt(D*N)*absTol + relTol * nystack

 		   if (prires <= eps_pri && dualres <= eps_dual) {
     		 break
    	 }
     	 iter+=1
     }//end while
     
     val elapsedTime = (System.nanoTime() - startTime) / 1e9
     
     println(s"Training time: $elapsedTime seconds")
     println(println(s"Coefficients: ${z.data}"))
     
    		  
  } //end parser
 

  def soft_threshold(v: BDV[Double], k: Double) = {
    v.map{ vi => {
        if(vi > k) vi-k
        else if (vi < -k) vi+k 
        else 0 }
    }
  } //end soft_threshold
  
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

 
