package prototypes.dfprototype

import scopt.OptionParser
import scala.io.Source
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
  
  def StrToDouble(str: String): Option[Double] = {
    try {
        Some(str.toDouble)
    } catch {
        case e: NumberFormatException => None
    }
  }
  
 /* def filterColumn(arr: Vector, cols: Array[Int]): Vector = {
    val zipped = arr.zipWithIndex
    val filtered = zipped.filterNot(x => cols.contains(x._2))
    return filtered.map(x => x._1)
  }
  
  def filterColumn(arr: Vector[String], cols: Array[Int], int: Int): Vector[String] = {
    val zipped = arr.zipWithIndex
    val filtered = zipped.filterNot(x => cols.contains(x._2))
    return filtered.map(x => x._1)
  }*/
  def main(args: Array[String]){
   // Logger.getLogger("org").setLevel(Level.OFF)
  // Logger.getLogger("akka").setLevel(Level.OFF)
      val spark = org.apache.spark.sql.SparkSession.builder
      .appName("Parser")
      .master("local")
      .getOrCreate()
    val a = parser(spark, args)
      
  }
  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    val t = mat.isTranspose

    if(!t)
      new DenseMatrix(mat.rows, mat.cols, mat.data)
    else {
      val densemat = new DenseMatrix(mat.rows, mat.cols, mat.data)
      densemat.transpose
    } 
  }
  def fromBreeze(vec: BDV[Double]): DenseVector =  new DenseVector(vec.data)
  
  def asBreeze(mat: DenseMatrix): BDM[Double] = {
    val t = mat.isTransposed
    if (!t)
      new BDM(mat.numRows, mat.numCols, mat.values)
    else {
      val breezeMatrix = new BDM(mat.numRows, mat.numCols, mat.values)
      breezeMatrix.t
    }
  }
  def asBreeze(vec: DenseVector): BV[Double] = new BDV[Double](vec.values)
  
  
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
        val temp = args(0).split(" ")  
     val vcols = for (string <- temp) yield {
      string.toInt-1
     }
      //val snp = CreateDataframe_2(spark, "resources/sampleSNP.txt")
      val snp = CreateDataframe(spark, "resources/sampleSNP.txt", true, vcols)
     
      //snp.createOrReplaceTempView("Epitasis")
     // snp.show()
      
     
      val phe = CreateDataframe(spark, "resources/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt", false, null)
      //phe.show()
      
     /* val snp2 = CreateDataframe_2(spark, "resources/sampleSNP.txt")
            snp2.show()
      val phe2 = CreateDataframe_2(spark, "resources/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt")
      phe2.show()
       *
       */
      
      spark.sparkContext.addFile("resources/sampleSNP.txt")
      spark.sparkContext.addFile("resources/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt")
      val data = Source.fromFile("resources/sampleSNP.txt").getLines().map(_.split("\t")).toArray
      val samples = data(0).drop(5)
      val snp_names = data.drop(1).map(x => x(0))
      var i = 0
      val snp_matrix = new DenseMatrix(snp_names.length, samples.length, data.drop(1).flatMap(x => x.drop(5).map(_.toDouble)))
      val values = spark.sparkContext.parallelize{
        data.drop(1).map{x => 
          val ret = IndexedRow(i, Vectors.dense(x.drop(5).map(_.toDouble)))
          i+=1
          ret
        }
      }
      
      val phefile = Source.fromFile("resources/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt").getLines().map(_.split("\t")).toArray
      val phe_samples = phefile.drop(1).map(x => x(0))
      val phe_names = phefile(0).drop(1)
      val phe_vales = phefile.drop(1).flatMap(x => x.drop(1).map(_.toDouble))
      
      println(phe_samples.length)
      println(phe_names.length)
      
      val phevals = new DenseMatrix(phe_samples.length, phe_names.length, phe_vales, true)

      val colIter = phevals.colIter
      val PH1 = colIter.next
      
      

      val yi = spark.sparkContext.broadcast(PH1)
      var rho = 1.0;
      val D = snp_names.length
      val N = samples.length
      val maxIter = 100
      val lambda = 0.5
      val skinny = (N >=D)
      //precompute normalization vector z: zj = SUM yj(xi)^2
      val Atb = new DenseVector(Array.fill[Double](D)(0))
      
      BLAS.gemv(1, snp_matrix.transpose, PH1, 0, Atb)
      
      var A =  new BDM(1, 1, Array(0.0))
      if (skinny){
        //Compute the matrix L: L = chol(AtA + rho*I) 
        val AtA = new DenseMatrix(D, D, Array.fill[Double](D*D)(0))
        BLAS.gemm(1, snp_matrix.transpose, snp_matrix, 0, AtA)
        
        val rhoI = asBreeze(DenseMatrix.eye(D))
        rhoI*=rho
        val L = asBreeze(AtA) + rhoI
        A = cholesky(L)
      } else{
      //compute the matrix L : L = chol(I + 1/rho*AAt) 
      val AAt = new DenseMatrix(N, N, Array.fill[Double](N*N)(0))
      
      BLAS.gemm(1, snp_matrix, snp_matrix.transpose, 0, AAt)
      BLAS.gemm(0, null, null, 1/rho, AAt)
      
      val eye = asBreeze(DenseMatrix.eye(N))
      val L = asBreeze(AAt)+eye
      // L = A*At
      A = cholesky(L)
      
      }
      
     val x = BDV.zeros[Double](D)
     val z = BDV.zeros[Double](D)
     val u = BDV.zeros[Double](D)
     val y = BDV.zeros[Double](D)
     val r = BDV.zeros[Double](D)
     val zprev = BDV.zeros[Double](D)
     val zdiff = BDV.zeros[Double](D)
     var q = BDV.zeros[Double](D)
     val w = BDV.zeros[Double](D)
     val Aq = new DenseVector(Array.fill[Double](N)(0))
     val p = BDV.zeros[Double](N)
  
     
 
    // val w = Array.fill[Double](D)(0); //Initialize array of 0 of size d for coefficients. 
     var prires: Double = 0; //primal residual
     var iter = 0
     //while (iter < maxIter){
       // u-update: u = u + x - z
       x-=z
       u+=x
      // x-update: x = (A^T A + rho I) \ (A^T b + rho z - y) 
      q = z.copy
      q-=u
      q*=rho
      q +=asBreeze(Atb)
      
      if(skinny){
        //TODO
        solve( A, q, x )
      }
      else{
        BLAS.gemv(1, fromBreeze(A), fromBreeze(q), 0, Aq)
        solve(A, Aq, p)
        
        /* x = q/rho - 1/rho^2 * A^T * (U \ (L \ (A*q))) 
211 			gsl_blas_dgemv(CblasNoTrans, 1, A, q, 0, Aq);
212 			gsl_linalg_cholesky_solve(L, Aq, p);
213 			gsl_blas_dgemv(CblasTrans, 1, A, p, 0, x); /* now x = A^T * (U \ (L \ (A*q)) */
214 			gsl_vector_scale(x, -1/(rho*rho));
215 			gsl_vector_scale(q, 1/rho);
216 			gsl_vector_add(x, q);*/
        
      }
      
      
       val a = values.mapPartitionsWithIndex{
         (index, iterator) => {
           val y = yi.value
           //iterator
           println("index", index)
           //val x = new DenseVector(Array(0.0,0.0))
           //val g = BLAS.gemv(1.0, A, x, 1.0, y)
            while (iterator.hasNext) {
              val a = iterator.next()
              //BLAS.gemv()
            }
            Iterator(y)
           }
       }
         iter+=1    
         spark.stop()
   //  }
      
    
  /*    val snpfile = spark.sparkContext.textFile("resources/sampleSNP.txt").map(x => x.split("\t"))
     // val samples = snpfile.first().drop(5)
     // val snp_names = snpfile.map(x => x(0)).collect().drop(1)
      val prevalues = snpfile.mapPartitionsWithIndex((idx, iter)  => if (idx ==0) iter.drop(1) else iter )
      val values = prevalues.map(x => x.map(_.toDouble))
      
      val phefile = spark.sparkContext.textFile("resources/sampleSNP.txt").map(x => x.split("\t"))
      val phe_names = phefile.map(x => x(0)).collect().drop(1)
      val prephevalues = snpfile.mapPartitionsWithIndex((idx, iter)  => if (idx ==0) iter.drop(1) else iter )
      val prephevalues2 = prephevalues.map(x => x.map(_.toDouble))
      
      val phevalues = { val byColumnAndRow = prephevalues2.zipWithIndex.flatMap {
                case(row, rowIndex) => row.zipWithIndex.map{
                  case(string, columnIndex) => columnIndex -> (rowIndex, string)
                }
              }
              val byColumn = byColumnAndRow.groupByKey.sortByKey().values
              byColumn.map {
                indexedRow => indexedRow.toArray.sortBy(_._1).map(_._2)
              }
      }
      
      val assembler = new VectorAssembler()
      .setInputCols(snp_names)
      .setOutputCol("all_snps")

      //  val LabeledPointsRDD = phevalues.mapPartitionsWithIndex{}
  val scaler = new StandardScaler()
        .setInputCol("assembled")
        .setOutputCol("features")
        .setWithStd(true)
        .setWithMean(true)
 
        val assembled = assembler.transform(snp.join(phe, "<Marker>"))
        val scalerModel = scaler.fit(assembled)
    
        
     val dataset = scalerModel.transform(assembled)
     dataset.show()
     
     val r = IndexedRow.
     val rows: RDD[IndexedRow] = spark.sparkContext.parallelize(Array[IndexedRow])
        
      val denseRDD = labeledPointsToMatrix(dataset, "PH1")
     
      val numRows = denseRDD.count  //num partitions
      val numFeatures = snp_names.size

    val initialWeights = Vectors.zeros(numFeatures)   
    val xy = newOptimizer.computeXY(denseRDD, numFeatures, numRows)
      (denseRDD, numRows, numFeatures)
      
      */
  }
  
   private def newOptimizer = new CoordinateDescent()
  
  def labeledPointsToMatrix(dataset: DataFrame, label: String) = {
    val rowRDD = dataset.select(label, "features").rdd
    .map {
      case Row(label: Double, features: Vector) =>
        (label, features.toArray)
    }

    val rowsColsPerPartition = rowRDD.mapPartitionsWithIndex {
      case (part, iter) =>
        if (iter.hasNext) {
          val nCols = iter.next()._2.size
          Iterator((part, 1 + iter.size, nCols))
        } else {
          Iterator((part, 0, 0))
        }
    }.collect().sortBy(x => (x._1, x._2, x._3)).map(x => (x._1, (x._2, x._3))).toMap

    
    val rBroadcast = rowRDD.context.broadcast(rowsColsPerPartition)
    val data = rowRDD.mapPartitionsWithIndex {
      case (part, iter) =>
        val (rows, cols) = rBroadcast.value(part)
        val vecData = new Array[Double](rows)
        val matData = new Array[Double](rows * cols)
        var nRow = 0
        while (iter.hasNext) {
          val row = iter.next()
          vecData(nRow) = row._1
          val arr = row._2
          var idx = 0
          while (idx < arr.size) {
            matData(nRow + idx * rows) = arr(idx)
            idx = idx + 1
          }
          nRow += 1
        }
        //TODO - Is vecData.toArray, matData.toArray needed?
        println(part,"rows: ", rows, "cols: ", cols)
        Iterator((new DenseVector(vecData), new DenseMatrix(rows, cols, matData)))
    }
    
    data
  }
  
 /* def CreateDataframe_obsolete(spark: SparkSession, file: String, t: Boolean, rmCols: Array[Int]): DataFrame = {

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
  }*/

def CreateDataframe_2(spark: SparkSession, file:String): DataFrame ={
    val fileRDD = spark.read
    .format("csv")
    .option("sep", "\t")
    .option("maxColumns", 500000)
    .option("header", "true")
    .csv(file)
    
    fileRDD
    
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


class CoordinateDescent{
  /* dataset: RDD of y and X
   * n: number of samples
   * d: dimensionality, or number of SNPs
   * lambda: regParam
   * tol: convergence tolerence level
   */
  def fit(X:RDD[IndexedRow], y: DenseVector,  n: Int, d: Int, lambda: Double, tol: Double, maxIter: Int) = {
     val w = Array.fill[Double](d)(0); //Initialize array of 0 of size d for coefficients. 
     var prires: Double = 0; //primal residual
     var iter = 0
     val z = X.context.broadcast(y)
     def layout(x: Double) = n + d +x
     while (iter < maxIter){
       val a = X.mapPartitionsWithIndex{
         (index, iterator) => {
           val y = z.value
           //iterator
           println("index", index)
           //val x = new DenseVector(Array(0.0,0.0))
           //val g = BLAS.gemv(1.0, A, x, 1.0, y)
            while (iterator.hasNext) 
              println(iterator.next())
            Iterator(Int)
           }
       }
         iter+=1    
     }
    
  }
  

  

  
  def soft_threshold(v: Array[Double], j: Int, lambda: Double, rho: Double) = {
     if (rho > lambda/2){
       v(j) = (rho-lambda/2)
     }
     else if (rho < -lambda/2){
       v(j) = (rho+lambda/2)
     }
     else v(j) =0
  }
  
  
  
}