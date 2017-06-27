package statistics

import scala.annotation.tailrec
import scala.collection.mutable.HashSet
import org.apache.commons.math3.distribution.TDistribution
import breeze.stats.regression.leastSquares
import breeze.stats.regression.LeastSquaresRegressionResult
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix

import org.apache.spark._

case class StepCollections(not_added: HashSet[String],
                           added_prev: HashSet[String] = HashSet(),
                           skipped: HashSet[String] = HashSet()
                          )

object Regressions {
    
  /** Creates a DenseMatrix from a 2D Vector of columns[rows]
   *  
   *  This requires a transposition, because breeze's matrices are stored as columns
   */
  private def createDenseMatrix(input: Vector[Vector[Double]]): DenseMatrix[Double] = {
    val numRows = input.transpose.length
    val numCols = input.transpose.apply(0).length
    new DenseMatrix(numRows, numCols, input.transpose.flatten.toArray)
  }
   
  def performLinearRegression(xColumnNames: Array[String],
                        yColumnName: String,
                        Xs: Vector[Vector[Double]],
                        Y: Vector[Double]
                       ): RegressionSummary = {
    
    // To estimate the intercept, a column of 1's is added to the matrix in the last position
    val XsWithZeroColumn = Xs :+ Vector.fill(Xs(0).size)(1.0)
    
    val xMatrix = createDenseMatrix(XsWithZeroColumn)
    val yVector = DenseVector(Y.toArray)

    val regressionResult = leastSquares(xMatrix, yVector)
    new RegressionSummary(xColumnNames, yColumnName, xMatrix, yVector, regressionResult) 
  }
  
  /*
  
  def main(args: Array[String]) {
    
    val a = Vector(1.0, 2.0, 4.0, 5.0, 6.0, 7.0, 8.0 )
    //val b = Vector(Vector(2.3), Vector(5.7), Vector(7.9), Vector(8.0), Vector(7.0), Vector(9.0), Vector(11.0))
    val b = Vector(Vector(2.3, 5.7, 7.9, 8.0, 7.0, 9.0,11.0))

    
    val output = performRegression(Array("po", "ro"), b, a)
    //output.coefficients.foreach(println)
    
    
    val xMatrix = new DenseMatrix(5, 3, Array(1,2,3,4,6,
                                              3.0,4.0,6.0,7.0,8.0,
                                              1,1,1,1,1)
                                             )

    //val xMatrix = new DenseMatrix(11,2, Array(1,2,3,4,5,6,7,8,9,10.0,11, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,1))
//    val xMatrix = new DenseMatrix(5,2, Array(1.0,2,3,4,6, 1,1,1,1,1))
    
    val printMatrix = (m: DenseMatrix[Double]) => 
      for (i <- 0 until m.rows) {
        for (j <- 0 until m.cols - 1) print(m(i,j) + ", ")
        println(m(i, m.cols - 1))
      }
    
    printMatrix(xMatrix)
    
    val yVector = new DenseVector(Array(1.1,2.2,3.3,4.4,5.5))
    //val yVector = new DenseVector(Array(1.0,2,3,4,5,6,7,8,8,10,13))
    val regressionResult = leastSquares(xMatrix, yVector)
    //regressionResult.coefficients.foreach(x => println("coefficient: " + x.toString()))

    val regSum = new RegressionSummary(Array("po"), "ro", xMatrix, yVector, regressionResult)
    
    val fitted = regSum.fittedValues
    val res = regSum.residuals
    
    val dof = regSum.degreesOfFreedom
    println("dof: " + dof)
   // println(regSum.residualStandardError)    
   //regSum.pValuesOfCoefficients.foreach(println)
   
   
   println((1 - new breeze.stats.distributions.StudentsT(3).cdf(math.abs(-3))) * 2)

   regSum.xDiffFromMean.map(_.foreach(x => println("diffs from mean: " + x)))
   //regSum.xMeans.foreach(x => println("x means: " + x))
   
   //regSum.regResult.coefficients.foreach(x => println("coeff: " + x))
   //regSum.residuals.foreach(println)
   regSum.standardErrorsOfCoefficients.foreach(x => println("StdErr: " + x))
   //regSum.tStatisticsOfCoefficients.foreach(x => println("t: " + x))

   //regSum.pValuesOfCoefficients.foreach(x => println("p-values: " + x))
  }*/
}