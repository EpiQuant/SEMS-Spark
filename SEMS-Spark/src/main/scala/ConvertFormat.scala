import scopt._
import java.io.File
import converters._

case class Config(input: File = new File("."), output: File = new File("."),
                  inputType: String = "Custom",
                  deleteColumns: Seq[Int] = Seq(),
                  transpose: Boolean = false,
                  delimiter: String = "\t"
                 )

object ConvertFormat {
  
  private val fileTypes = List("plink", "Plink", "PLINK",
                         "Custom", "custom"
                         )
  
  private val argParser = new scopt.OptionParser[Config]("ConvertFormat") {
    head("\nConvertFormat")

    note("Required Arguments\n------------------")
      
    opt[File]('i', "input")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(input = x) )
      .text("Path to the file to convert")
        
    opt[File]('o', "output")
      .required
      .valueName("<file>")
      .action( (x, c) => c.copy(output = x) )
      .text("Path to where the output file will be placed")
        
    opt[String]("inputType")
      .required
      .valueName("<String>")
      .action( (x, c) => c.copy(inputType = x) )
      .text("The format of the input file { plink | custom }")
      .validate {x =>
        if (fileTypes.contains(x)) success
        else failure("File type must be plink, ..., or custom")
      }
      
    note("\nOptional Arguments\n------------------")

    opt[String]('d', "delimiter")
      .optional
      .valueName("{ default = <tab> }")
      .text("Set what delimiter to use")
      // Allows user to enter \t for a tab, although technically the program needs \\t
      .action{ 
        (x, c) => {
          if (x == "t") c.copy(delimiter = "\\t")
          else c.copy(delimiter = x )
        }
      }
    
    opt[Boolean]('t', "transpose")
      .optional
      .valueName("{ default = false }")
      .text("Transpose the data")
      .action( (x, c) => c.copy(transpose = x) )

    opt[Seq[Int]]("deleteColumns")
      .optional
      .valueName("<Int>,<Int>,...")
      .text("Comma separated list of columns to delete; Count from 0")
      .action( (x, c) => c.copy(deleteColumns = x) )
  }

  def main(args: Array[String]) = { 
    
    val parsed = argParser.parse(args, Config())
    
    parsed match {
      
      // Handle a case where there is something wrong with the input arguments
      case None => {
        System.err.println("\nError: Invalid/incomplete arguments")
      }
      
      // If there is a valid set of arguments presented
      case Some(config) => {
        
        // FileParser is an abstract class
        val parser: FileParser = parsed.get.inputType match {
          case "custom" | "Custom" | "CUSTOM" => {
            new CustomFileParser(parsed.get.input,
                                 parsed.get.delimiter,
                                 parsed.get.deleteColumns,
                                 parsed.get.transpose
                                )
          }
          
          case _ => throw new Error("Error: Invalid Input Type")
        }
    
        parser.saveParsedFile(parsed.get.output)
        println("Conversion successful: new file can be found at: " + parsed.get.output.getAbsolutePath.toString)
        
      }
    }
  }
}