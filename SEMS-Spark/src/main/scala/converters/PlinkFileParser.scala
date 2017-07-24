package converters

import java.io.File

abstract class PlinkFileParser(filePath: File) extends FileParser(filePath) {

  def saveParsedFile
  
}