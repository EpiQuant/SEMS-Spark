package statistics

import org.junit.Assert._
import org.junit._
import scala.Vector

object OLSRegressionTest {

  var simpleReg: OLSRegression = _
  var multiReg: OLSRegression = _

  @BeforeClass def initialSetup {

    val X1 = Vector(1.0, 2, 3, 5, 7)
    val X2 = Vector(6.0, 4, 3, 4, 1)
    val X3 = Vector(100.0, 156, 146, 398, 201) 
    
    val simpleX = Vector(X1)
    val multiX = Vector(X1, X2, X3)
    
    val Y = Vector(1.0, 2, 3, 4, 5)
    
    simpleReg = new OLSRegression(Array("X1"), "Y", simpleX, Y)
    
    multiReg =
      new OLSRegression(Array("X1", "X2", "X3"), "Y", multiX, Y)
  }
}

class OLSRegressionTest {

  /*
   * SIMPLE LINEAR REGRESSION TESTS
   * 
   * Need to agree with the following R output

# Code
x <- c(1,2,3,5,7)
y <- c(1,2,3,4,5)
summary(lm(y~x))

Call:
lm(formula = y ~ x)

Residuals:
       1        2        3        4        5 
-0.31897  0.03448  0.38793  0.09483 -0.19828 

Coefficients:
            Estimate Std. Error t value Pr(>|t|)   
(Intercept)  0.67241    0.27622   2.434  0.09297 . 
x            0.64655    0.06584   9.820  0.00224 **
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Residual standard error: 0.3171 on 3 degrees of freedom
Multiple R-squared:  0.9698,	Adjusted R-squared:  0.9598 
F-statistic: 96.43 on 1 and 3 DF,  p-value: 0.002245

  */
  
  @Test def simpleCoefficientsTest {
    val coefficients = OLSRegressionTest.simpleReg.coefficients
    
    // Two estimates should be present: one for the slope and one for the intercept
    assertEquals(coefficients.length, 2)
    val X1 = coefficients(0)
    val intercept = coefficients(1)
    
    assertTrue(X1 > 0.64654 && X1 < 0.64656)
    assertTrue(intercept > 0.67240 && intercept < 0.67242)
  }
  
  @Test def simpleStandardErrorTest {
    val stdErrors = OLSRegressionTest.simpleReg.standardErrors
    
    assertEquals(stdErrors.length, 2)
    val X1 = stdErrors(0)
    val intercept = stdErrors(1)
    
    assertTrue(X1 > 0.06583 && X1 < 0.06585)
    assertTrue(intercept > 0.27621 && intercept < 0.27623)
  }
  
  @Test def simpleTStatisticTest {
    val tStats = OLSRegressionTest.simpleReg.tStatistics
    
    assertEquals(tStats.length, 2)
    val X1 = tStats(0)
    val intercept = tStats(1)
    
    assertTrue(X1 > 9.819 && X1 < 9.821)
    assertTrue(intercept > 2.433 && intercept < 2.435)
  }
  
  @Test def simplePValueTest {
    val pVals = OLSRegressionTest.simpleReg.pValues
    
    assertEquals(pVals.length, 2)
    val X1 = pVals(0)
    val intercept = pVals(1)
    
    assertTrue(X1 > 0.00223 && X1 < 0.00225)
    assertTrue(intercept > 0.09296 && intercept < 0.09298)
  }
    /*
   * MULTIPLE LINEAR REGRESSION TESTS
   * 
   * Should agree with the following R output

x1 <- c(1,2,3,5,7)
x2 <- c(6,4,3,4,1)
x3 <- c(100,156,146,398,201)
y <- c(1,2,3,4,5)
summary(lm(y~x1+x2+x3))

Call:
lm(formula = y ~ x1 + x2 + x3)

Residuals:
       1        2        3        4        5 
 0.02357 -0.17689  0.17543  0.02066 -0.04277 

Coefficients:
             Estimate Std. Error t value Pr(>|t|)
(Intercept)  2.351060   0.998613   2.354    0.256
x1           0.340324   0.169190   2.011    0.294
x2          -0.339646   0.186496  -1.821    0.320
x3           0.003229   0.001900   1.700    0.339

Residual standard error: 0.2547 on 1 degrees of freedom
Multiple R-squared:  0.9935,	Adjusted R-squared:  0.974 
F-statistic: 51.05 on 3 and 1 DF,  p-value: 0.1024

   */
  
  @Test def multiCoefficientsTest {
    val coefficients = OLSRegressionTest.multiReg.coefficients
    
    // Four estimates should be present: three slopes and one for the intercept
    assertEquals(coefficients.length, 4)
    val X1 = coefficients(0)
    val X2 = coefficients(1)
    val X3 = coefficients(2)
    val intercept = coefficients(3)
    
    assertTrue(X1 > 0.340323 && X1 < 0.340325)
    assertTrue(X2 > -0.339647 && X2 < -0.339645)
    assertTrue(X3 > 0.003228 && X3 < 0.003230)
    assertTrue(intercept > 2.351059 && intercept < 2.351061)
  }
  
  @Test def multiStandardErrorTest {
    val stdErr = OLSRegressionTest.multiReg.standardErrors
    
    assertEquals(stdErr.length, 4)
    val X1 = stdErr(0)
    val X2 = stdErr(1)
    val X3 = stdErr(2)
    val intercept = stdErr(3)
    
    assertTrue(X1 > 0.169189 && X1 < 0.169191)
    assertTrue(X2 > 0.186495 && X2 < 0.186497)
    assertTrue(X3 > 0.001899 && X3 < 0.001901)
    assertTrue(intercept > 0.998612 && intercept < 0.998614)
  }
    
  @Test def multiTStatisticTest {
    val tStats = OLSRegressionTest.multiReg.tStatistics
    
    assertEquals(tStats.length, 4)
    val X1 = tStats(0)
    val X2 = tStats(1)
    val X3 = tStats(2)
    val intercept = tStats(3)
    
    assertTrue(X1 > 2.010 && X1 < 2.012)
    assertTrue(X2 > -1.822 && X2 < -1.820)
    assertTrue(X3 > 1.699 && X3 < 1.701)
    assertTrue(intercept > 2.353 && intercept < 2.355)
  }
  
  @Test def multiPValueTest {
    val pVals = OLSRegressionTest.multiReg.pValues
    
    assertEquals(pVals.length, 4)
    val X1 = pVals(0)
    val X2 = pVals(1)
    val X3 = pVals(2)
    val intercept = pVals(3)
    
    assertTrue(X1 > 0.293 && X1 < 0.295)
    assertTrue(X2 > 0.319 && X2 < 0.321)
    assertTrue(X3 > 0.338 && X3 < 0.340)
    assertTrue(intercept > 0.255 && intercept < 0.257)
  }
}