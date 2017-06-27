package statistics

import breeze.linalg.DenseVector
import breeze.linalg.DenseMatrix
import breeze.linalg._
import breeze.stats.regression.LeastSquaresRegressionResult
import breeze.stats.distributions.StudentsT

class RegressionSummary(xColumnNames: Array[String],
                        yColumnName: String, 
                        Xs: DenseMatrix[Double],
                        Y: DenseVector[Double],
                        regResult: LeastSquaresRegressionResult) {
  // This class assumes the last coefficient is the intercept
  
  val N = Y.size
  val k = Xs.cols
  
  // N - k (k is the number of estimations; this assumes that there is a 1's column for the intercept)
  val degreesOfFreedom = N - k

  /** predicted Y (Y_hat) */
  lazy val fittedValues = calculateFittedValues
  
  private[this] def calculateFittedValues: DenseVector[Double] = {
    // The last item in the list of coefficients is the intercept
    val intercept = regResult.coefficients(regResult.coefficients.length - 1)

    val fittedValues = {
      for (rowIndex <- 0 until N) yield {
        // Subtract 1 since the last column is the intercept column
        val terms = for (colIndex <- 0 until k - 1) yield {
          // multiply by the coefficient for that column
          Xs(rowIndex, colIndex) * regResult.coefficients(colIndex)
        }
        // return the sum of each term with the intercept added
        terms.sum + intercept
      }
    }
    // The :_* unpacks the list into arguments to pass to DenseVector's apply method
    DenseVector(fittedValues:_*)
  }
  
  /** Difference between the actual and predicted Y values */ 
  lazy val residuals = Y - fittedValues
  
  // mean(X)
  lazy val xMeans: DenseVector[Double] = {
    val means = for (colIndex <- 0 until k) yield {
      val colValues = for (rowIndex <- 0 until N) yield {
        Xs(rowIndex, colIndex)
      }
      colValues.sum / N
    }
    // The :_* unpacks the list into arguments to pass to DenseVector's apply method
    DenseVector(means:_*)
  }

  lazy val xDiffFromMean:Array[DenseVector[Double]] = {
    for (col <- 0 until k) yield {
      val diffs = for (row <- 0 until N) yield {
        Xs(row, col) - xMeans(col)
      }
      DenseVector(diffs:_*)
    }
  }.toArray
    
  lazy private[this] val sum = (i: DenseVector[Double]) => i.reduce((x,y) => x + y)
  lazy private[this] val sumOfSquared = (i: DenseVector[Double]) => sum( i.map(math.pow(_, 2)) )
    
  //  SE of regression slope = sqrt [ Σ(yi – ŷi)^2 / (n – k) ] / sqrt [ Σ(xi – x)^2 ]
  lazy val standardErrorsOfCoefficients = {
        
    println("sigma squared: " + residualStandardError)        // Check
    //v//al denominatorCollection = sumOfSquaredDiffBetweenXandMeanX.map(math.sqrt(_))
    sumOfSquaredDiffBetweenXandMeanX.map(x => residualStandardError / math.sqrt(x))
    
    // Computes SE for all but the intercept
        
    // For the intercept
    
    // http://courses.ncssm.edu/math/Talks/PDFS/Standard%20Errors%20for%20Regression%20Equations.pdf
  }

  /*val standardErrorOfIntercept = {
    val sumOfSquaredResiduals = sumOfSquared(residuals)
    val sumOfSquaredDiffBetweenXandMeanX = xDiffFromMean.map(sumOfSquared(_))
    val sum = sumOfSquaredDiffBetweenXandMeanX.sum
    
    val sigma_squared = sumOfSquaredResiduals / degreesOfFreedom
    
    sumOfSquared
    }*/
  /*val standardErrorOfIntercept = {
    val sumOfSquaredResiduals = sumOfSquared(residuals)
    val sumOfSquaredDiffBetweenXandMeanX = xDiffFromMean.map(sumOfSquared(_))
    
    val sigma_squared = math.sqrt(sumOfSquaredResiduals / degreesOfFreedom)
    val denominatorCollection = sumOfSquaredDiffBetweenXandMeanX.map(math.sqrt(_))
        
    // Computes SE for all but the intercept
    sigma_squared / denominatorCollection.last
  }*/
  
  lazy val residualStandardError = {
    math.sqrt( (sumOfSquared(residuals) / degreesOfFreedom) )
  }
  
  lazy val sumOfSquaredDiffBetweenXandMeanX = xDiffFromMean.map(sumOfSquared(_))
  
  lazy val tStatisticsOfCoefficients = {
    // Loop through all coefficients but the intercept
    val Ts = for (i <- 0 until regResult.coefficients.length - 1) yield {
      regResult.coefficients(i) / standardErrorsOfCoefficients(i)
    }
    // The :_* unpacks the list into arguments to pass to DenseVector's apply method
    DenseVector(Ts:_*)
  }
  
  val tStatistic2pValue = (t: Double) => {
    // Two-tailed test
    (1 - new StudentsT(degreesOfFreedom).cdf(math.abs(t))) * 2
  }
  
  lazy val pValuesOfCoefficients = {
    // Loop through all of the t-Statistics but for the intercept
    tStatisticsOfCoefficients.map(tStatistic2pValue(_))
  }
  
}