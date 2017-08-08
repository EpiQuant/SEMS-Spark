import java.io.File
import scopt._
import prototypes.rddprototype.RDDPrototype
import org.apache.spark.sql.SparkSession

case class InputConfig(genotypeInput: String = ".",
                       phenotypeInput: String = ".",
                       output: String = ".",
                       forwardThres: Double = 0.05,
                       backwardThres: Double = 0.05
                      )

object StepwiseModelSelection {

  private val argParser = new scopt.OptionParser[InputConfig]("StepwiseModelSelection") {
    head("StepwiseModelSelection")

    note("Required Arguments\n------------------")
      
    opt[String]('G', "genotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(genotypeInput = x) )
      .text("Path to the genotype file")
    
    opt[String]('P', "phenotypeInput")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(phenotypeInput = x) )
      .text("Path to the phenotype file")
      
    opt[String]('o', "output")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(output = x) )
      .text("Path to where the output file will be placed")
      
    note("\nOptional Arguments\n------------------")

    opt[Double]("forwardThreshold")
      .optional
      .valueName("<file>")
      .action( (x, c) => c.copy(forwardThres = x) )
      .text("The p-value threshold for the forward step")
    
    opt[Double]("backwardThreshold")
      .optional
      .valueName("<file>")
      .action( (x, c) => c.copy(backwardThres = x) )
      .text("The p-value threshold for the backward step")
  }
  
  def main(args: Array[String]) {
    
    val parsed = argParser.parse(args, InputConfig())
      
    parsed match {
      
      // Handle a case where there is something wrong with the input arguments
      case None => {
        System.err.println("\nError: Invalid/incomplete arguments")
      }
      
      // If there is a valid set of arguments presented
      case Some(config) => {

         //val spark = SparkSession.builder.appName("SEMS").getOrCreate()
        val spark = SparkSession.builder.appName("SEMS").master("local").getOrCreate()
        
        RDDPrototype.performSEMS(spark,
                                 parsed.get.genotypeInput,
                                 parsed.get.phenotypeInput,
                                 parsed.get.output,
                                 parsed.get.forwardThres,
                                 parsed.get.backwardThres
                                )
      }
    }  
  }
}