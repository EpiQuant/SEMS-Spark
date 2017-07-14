package converters

class CustomFileParser(filePath: String,
                       outputPath: String,
                       delimiter: String = "\t",
                       columnsToDelete: IndexedSeq[Int] = IndexedSeq(),
                       transpose: Boolean = false
                      ) 
        extends FileParser(filePath, outputPath) {
  
  private val table = this.readFile(filePath, delimiter)
  
  /*private def convertInputTable: Table = {
    if (transpose) {
      table.
    }
    else {
    
    }
  }*/
  
}