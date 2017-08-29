package converters

import java.io.File

class CustomFileParser(filePath: File,
                       delimiter: String = "\\t",
                       columnsToDelete: Seq[Int] = Seq(),
                       transpose: Boolean = false
                      ) 
        extends FileParser(filePath) {
  
  private val table = this.readFile(filePath, delimiter)
  
  private val convertedTable = {
    val filtered = table.deleteColumns(columnsToDelete: _*)
    if (transpose) filtered.transpose else filtered
  }
  
  def saveParsedFile(outputPath: File) = {
    convertedTable.replaceTopLeftEntry.saveTableAsTSV(outputPath)
  }
}