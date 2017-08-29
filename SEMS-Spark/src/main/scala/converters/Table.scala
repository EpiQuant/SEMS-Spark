package converters

import scala.collection.immutable.Map
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

class Table(val table: Vector[Vector[Any]]) {
    
  def transpose: Table = {
    new Table(table.transpose)
  }
  
  def replaceTopLeftEntry: Table = {
    val firstLine = "HeaderLine" +: table(0).drop(1)
    val otherLines = table.drop(1)
    new Table(firstLine +: otherLines)
  }
  
  private val rowToString = (input: Vector[Any]) => {
    var string = ""
    for (i <- 0 until input.length - 1) {
      string = string + input(i).toString + "\t"
    }
    string = string + input.last.toString + "\n" 
    string
  }
  
  def saveTableAsTSV(file: File) {
    val bw = new BufferedWriter(new FileWriter(file))
    
    this.table.foreach( x => {
      bw.write(rowToString(x))
    })
    bw.close()
  }
  
  def printTable {
    for (row <- table.dropRight(table.size - 5)) {
      row.dropRight(1).foreach(entry => print(entry + "\t"))
      println(row.last)
    }
  }

  private[this] def invertMap(oldMap: Map[String,
                              Vector[Double]]
                             ): scala.collection.mutable.Map[Vector[Double], List[String]] = {
    val newMap = scala.collection.mutable.Map.empty[Vector[Double], List[String]]
    // Add the values mapped to empty vectors
    oldMap.foreach{ x => newMap += (x._2 -> List.empty) }
    // Add the old keys to the map in the value position
    oldMap.foreach{ x => newMap(x._2) = newMap(x._2) :+ x._1 }
    newMap
  }
  
  lazy val printNames = (i: List[String]) => {
    i.size match {
      case 2 => print(i(0) + " and " + i(1))
      case _ => {
        i.dropRight(1).foreach(x => print(x + ", "))
        print("and " + i.last)
      }
    }
  }
  
  lazy val nameIndexMap = getColumnNames.zipWithIndex.toMap
  
  def filterRedundantColumns: Table = {

    val colList = createColumnList
    val valList = colList.map(_._2)
    
    // If there are any columns with identical values
    if (valList.distinct.length < valList.length) {
      // Invert map from colNames -> [values] to [values] -> List[colNames]
      val inverted = invertMap(createColumnMap)
            
      val toDeleteLists: Iterable[List[String]] = for (key <- inverted.keys) yield {
        val names = inverted(key)
        if (names.length > 1) {
          println("WARNING: These columns have identical values: ")
          printNames(names)
          println("\nAll but the first of these will be removed from the analysis")
          names.drop(1) // All but the last are returned
        }
        else {
          List() // This is just a placeholder
        }
      }
      val toDelete = toDeleteLists.flatten.toArray
      val toDeleteIndices = toDelete.map(nameIndexMap(_))
    
      // The :_* unpacks the contents of the array as arguments
      this.deleteColumns(toDeleteIndices :_ *)
    }
    else {
      // Do not change anything
      this
    }
  }
  
  /** Select a column based on the columns index, starting from 0 */
  def selectColumn(col: Int): Vector[Any] = {
    table.transpose.apply(col)
  }
  
  /** Select a column based on the columnName */
  def selectColumn(colName: String): Vector[Any] = {
    val map = this.getColumnNames.zipWithIndex.toMap
    val columnIndex = map(colName)
    this.selectColumn(columnIndex)
  }
  
  /**
   * Puts each row of the table in the order defined by the input string list
   * 
   * Used to make sure the samples in the SNP table and phenotype tables are
   *   in the same order
   */
  def sortRowsByNameList(sampleOrder: Vector[String]): Table = {

    val rowMap = this.createRowMap
    
    // This is a closure (free variable is rowMap)
    val createRowFunc = (i: String) => (i +: rowMap(i)).toVector
    /*
     * Check to be sure that the samples present in this table match
     *   the samples provided in the input list.
     * The "Sample" column name itself is filtered out when this comparison
     *   is made.
     */
    val tablesRowNames = rowMap.keys.filter(_ != "Sample").toList.sorted
    if (tablesRowNames == sampleOrder.sorted) {
      val outputRows = ("Sample" +: sampleOrder).map(createRowFunc)
      return new Table(outputRows)
    }
    else {
      throw new Exception("Samples do not match between the input tables")
    } 
  }
  
  /** Filters out columns from the input 2D Vector */
  def deleteColumns(columns: Int*): Table = {
    
    /** Filters specified columns out of the input row, and returns the filtered row
     *  Count from 0
     */
    val filterColumn = (arr: Vector[Any], cols: Seq[Int]) => {
      val zipped = arr.zipWithIndex
      val filtered = zipped.filterNot(x => cols.contains(x._2))
      filtered.map(_._1)
    }
    
    val filtered = for (i <- 0 until table.length) yield {
      filterColumn(table(i), columns)
    }
    return new Table(filtered.toVector)
  }
  
  /** Returns a collection of the column names, except for the first column */
  def getColumnNames: Vector[String] = {
    table.head.drop(1).map(_.toString)
  }
  
    /**
   * Creates a map where the key is the column name and the values are the column's values
   * 
   * This function leaves off the sample names column (the 1st column) itself
   */
  def createColumnMap: Map[String, Vector[Double]] = {
    this.createColumnList.toMap
  }
  
  /**
   * Creates a list of tuples for each column, except for the sample name column itself
   * 
   *  The first entry in each item is the column name and 
   *    the second is the column's values
   */
  def createColumnList: Vector[(String, Vector[Double])] = {
    // The 1st entry (the sample name column) is skipped
    val list = for (i <- 1 until table(0).size) yield {
      val column = this.selectColumn(i)
      (column(0).toString, column.drop(1).map(_.toString.toDouble))
    }
    list.toVector
  }
 
  def createRowMap: Map[String, Vector[Any]] = {
    // Make map where rowName -> vector of rows items (excluding the name itself)
    val rowMap = this.table.drop(1).map(x => x(0).toString() -> x.drop(1)).toMap
    // The header row itself is added, with Sample added as the key
    rowMap + ("Sample" -> this.table(0).drop(1))
  }
  
  /** Will perform an inner join on two tables based on the first column 
   def join(otherTable: Table): Table = {
     
    // Check whether the rows in the join column are fully compatible between the two tables
    val thisNameColumn = this.selectColumn(0).map(_.toString())
    val otherNameColumn = otherTable.selectColumn(0).map(_.toString())
    if (thisNameColumn != otherNameColumn) throw new Exception("Join cannot be performed, name columns are incompatible")
    else {
      val thisMap = this.createRowMap
      val otherMap = otherTable.createRowMap
            
      val headerRow = ("Sample" +: thisMap("Sample")) ++ otherMap("Sample")
      val otherRows = thisNameColumn.drop(1).map(x => (x +: thisMap(x)) ++ otherMap(x))
      
      new Table(headerRow +: otherRows)
    }
  }
  */
}