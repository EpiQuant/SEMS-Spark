package prototypes.rddprototype

class Table(table: Vector[Vector[Any]]) {
    
  def transpose: Table = {
    new Table(table.transpose)
  }
  
  def printTable {
    for (row <- table) {
      row.dropRight(1).foreach(entry => print(entry + "\t"))
      println(row.last)
    }
  }

  def selectColumn(col: Int): Vector[Any] = {
    table.transpose.apply(col)
  }
  
  /** Filters out columns from the input 2D Vector */
  def deleteColumns(columns: Int*): Table = {
    
    /** Filters specified columns out of the input row, and returns the filtered row
     *  Count by 0
     */
    def filterColumn(arr: Vector[Any], cols: Seq[Int]): Vector[Any] = {
      val zipped = arr.zipWithIndex
      val filtered = zipped.filterNot(x => cols.contains(x._2))
      return filtered.map(_._1)
    }
    
    val filtered = for (i <- 0 until table.length) yield {
      filterColumn(table(i), columns)
    }
    return new Table(filtered.toVector)
  }
  
  private[Table] def createMap: Map[String, Vector[Any]] = {
    // Make map where rowName -> vector of rows items (excluding the name itself)
    val rowMap = this.table.drop(1).map(x => x(0).toString() -> x.drop(1)).toMap
    // The header row itself is added, with Sample added as the key
    rowMap + ("Sample" -> this.table(0).drop(1))
  }
  
  /** Will perform an inner join on two tables based on the first column */
   def join(otherTable: Table): Table = {
     
    // Check whether the rows in the join column are fully compatible between the two tables
    val thisNameColumn = this.selectColumn(0).map(_.toString())
    val otherNameColumn = otherTable.selectColumn(0).map(_.toString())
    if (thisNameColumn != otherNameColumn) throw new Exception("Join cannot be performed, name columns are incompatible")
    else {
      val thisMap = this.createMap
      val otherMap = otherTable.createMap
            
      val headerRow = ("Sample" +: thisMap("Sample")) ++ otherMap("Sample")
      val otherRows = thisNameColumn.drop(1).map(x => (x +: thisMap(x)) ++ otherMap(x))
      
      new Table(headerRow +: otherRows)
    }
  }
  
}