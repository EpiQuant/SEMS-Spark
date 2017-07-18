package converters

import scala.io.Source
import java.io.File

abstract class FileParser(filePath: File) {
  
  /** Reads in a <delimiter> separated file, and returns a new 2D Vector of its contents */
  protected def readFile(filePath: File, delimiter: String): Table = {
    val buffSource = Source.fromFile(filePath)
    return new Table(buffSource.getLines.toVector.map(_.split(delimiter).toVector))
  }
   
  def saveParsedFile(outputPath: File)
    
}