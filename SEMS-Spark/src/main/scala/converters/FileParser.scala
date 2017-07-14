package converters

import scala.io.Source

abstract class FileParser(filePath: String, outputPath: String) {
  
  /** Reads in a <delimiter> separated file, and returns a new 2D Vector of its contents */
  protected def readFile(filePath: String, delimiter: String): Table = {
    val buffSource = Source.fromFile(filePath)
    return new Table(buffSource.getLines.toVector.map(_.split(delimiter).toVector))
  }
   
  def saveParsedFile(newPath: String)
    
}