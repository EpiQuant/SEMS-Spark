package prototypes.rddprototype

import prototypes.dfprototype.Parser
import scala.io.Source

object RDDPrototype {
  
  /** Reads in a <delimiter> separated file, and returns a new 2D Vector of its contents */
  def readFile(filePath: String, delimiter: String): Table = {
    val buffSource = Source.fromFile(filePath)
    return new Table(buffSource.getLines.toVector.map(_.split(delimiter).toVector))
  }

  def main(args: Array[String]) = {
    
    //val a = new Table(Vector(Vector("Sample", "Col1", "Col2"), Vector("po", 1, 2), Vector("ro", 3, 4)))
    //val b = new Table(Vector(Vector("Sample", "Y", "Z"), Vector("po", 9, 8), Vector("ro", 7, 6)))
    
    val snpTable = readFile("/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/1106_Markers_NAM_Kernel_Color_Families_Only_for_R_10SNPs.txt",
                            "\t"
                           ).deleteColumns(1,2,3,4).transpose
    val phenoTable = readFile("/Users/jacobheldenbrand/Documents/Spark_playground/Angela_test_data/Simulated.Data.100.Reps.Herit.0.5_1Pheno.txt",
                              "\t"
                             )

    
    snpTable.printTable
    phenoTable.printTable
    
    snpTable.join(phenoTable).printTable
    
    
    
    
  }
  
  
  
}