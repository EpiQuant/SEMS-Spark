package statistics

import org.apache.commons.math3.distribution.TDistribution
import breeze.stats.regression._
import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix

object Regressions {
  
  private def createDenseMatrix(input: Vector[Vector[Double]]): DenseMatrix[Double] = {
    val numRows = input.length
    val numCols = input(0).length
    new DenseMatrix(numRows, numCols, input.flatten.toArray)
  }
  
  /**
   *  Perform Linear Regression between the input vectors
   *  
   *  Calculate coefficients using Ordinary Least Squares method
   *  
   *  Get T-statistic
   *  get StdErr
   *  
   *  get p-value
   *  
   */
  def performRegression(columnNames: IndexedSeq[String], Xs: Vector[Vector[Double]], Y: Vector[Double]): LeastSquaresRegressionResult = {
    
    val xMatrix = createDenseMatrix(Xs)
    val yVector = DenseVector(Y.toArray)
    
    
    val a: LeastSquaresRegressionResult = leastSquares(xMatrix, yVector)
    a
    
  }
  
  def main(args: Array[String]) {
    
    val a = Vector(1.0, 2.0, 4.0, 5.0, 6.0, 7.0, 8.0 )
    val b = Vector(Vector(2.3, 5.7, 7.9, 8.0, 7.0, 9.0, 11.0))
    
    val output = performRegression(Array("po", "ro"), b, a)
    
  }
  
}