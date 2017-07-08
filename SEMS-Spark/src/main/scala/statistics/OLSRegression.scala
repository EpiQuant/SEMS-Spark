package statistics

import breeze.linalg._
import breeze.stats.regression.LeastSquaresRegressionResult
import breeze.stats.distributions.StudentsT

class OLSRegression(val xColumnNames: Array[String],
                    val yColumnName: String, 
                    val Xs: scala.Vector[scala.Vector[Double]],
                    val Y: scala.Vector[Double]
                   ) {
  // Good summary of formula's used
  // http://www.stat.ucla.edu/~nchristo/introeconometrics/introecon_matrixform.pdf
  
  private[this] val yAsBreezeVector = DenseVector(Y.toArray)
  
  // To estimate the intercept, a column of 1's is added to the matrix in the last position
  private[this] val XsWithZeroColumn = {
    val numRows = Y.size
    val numCols = Xs.transpose.apply(0).size
    val ones = List.fill(numRows)(1.0)
    new DenseMatrix(numRows, numCols + 1, Xs.flatten.toArray ++ ones)
  }  
  
  val N = Y.size
  private[this] val k = XsWithZeroColumn.cols

  // N - k (k is the number of estimations; this assumes that there is a 1's column for the intercept)
  val degreesOfFreedom = N - k

  private[this] val transposedX = XsWithZeroColumn.t
  private[this] val inverseOfXtimesXt = inv(transposedX * XsWithZeroColumn)

  /** The estimates of the coefficients; the last entry is the estimate of the intercept */
  val coefficients = (inverseOfXtimesXt * transposedX * yAsBreezeVector).toArray
  
  /** 
   *  Predicted Y values, also known as Y_hat
   *  
   *  Y_hat = H Y
   *  where H is the hat matrix: H = X (X'X)^-1 X'
   */
  val fittedValues = XsWithZeroColumn * inverseOfXtimesXt * transposedX * yAsBreezeVector
 
  /** Difference between the actual and predicted Y values */ 
  val residuals = yAsBreezeVector - fittedValues
  
  val residualStandardError = math.sqrt( (sumOfSquared(residuals) / degreesOfFreedom) )
  
  /** Standard error for each coefficient; the final entry is for the intercept */
  val standardErrors = diag(inverseOfXtimesXt).toArray.map(math.sqrt(_) * residualStandardError)
  
  /** T-statistic for each coefficient; the final entry is for the intercept */
  val tStatistics = for (i <- 0 until k) yield { coefficients(i) / standardErrors(i) }

  /** Performs Two-tailed test and gets a p-value from the T-statistic */
  private[this] val tStatistic2pValue = (t: Double) => (1 - new StudentsT(degreesOfFreedom).cdf(math.abs(t))) * 2
  
  /** p-value for each coefficient; the final entry is for the intercept */
  val pValues = tStatistics.map(tStatistic2pValue(_)).toList
  
  lazy private[this] val sum = (i: DenseVector[Double]) => i.reduce((x,y) => x + y)
  lazy private[this] val sumOfSquared = (i: DenseVector[Double]) => sum( i.map(math.pow(_, 2)) )
    
  /** Key is the name of the X variable, the value is the p-value associated with it */
  lazy val pValueMap = (xColumnNames :+ "intercept").zip(pValues).toMap
  
  lazy val lastXColumnsValues = {
    // Last column position (the very last position is the 1's column used to estimate the intercept)
    val pos = k - 1
    (0 until N).map(XsWithZeroColumn(_, pos)).toVector
  }
  
  /** Prints a summary of the regression, in a format similar to R's summary */
  def printSummary {
    
    def standardizeLengths(arr: Array[String], rightPadding: Boolean = false) = {
      val maxLength = arr.map(_.length).max
      val padRight = (i: String) => i + " " * (maxLength - i.length)
      val padLeft = (j: String) => " " * (maxLength + 3 - j.length) + j
      if (rightPadding) arr.map(padRight) else arr.map(padLeft)
    }

    println("Y is " + yColumnName + "\n")
    
    val names = "Name" +: xColumnNames :+ "(Intercept)"
    // The formatting below chops each double to show only a few decimal places
    val estimate = "Estimate" +: coefficients.map(x => f"$x%.6f".toString)
    val stdErr = "Std. Error" +: standardErrors.map(x => f"$x%.6f".toString)
    val tStat = "t value" +: tStatistics.toArray.map(x => f"$x%.3f".toString)
    val pValue = "Pr(>|t|)" +: pValues.toArray.map(x => f"$x%.3f".toString)
    
    val cols = Array(standardizeLengths(names, rightPadding = true),
                     standardizeLengths(estimate),
                     standardizeLengths(stdErr),
                     standardizeLengths(tStat),
                     standardizeLengths(pValue)
                    )
                     
    val printRow = (row: Array[String]) => (row :+ "\n").foreach(print)
    cols.transpose.foreach(printRow)
  }
}