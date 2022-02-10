package alan.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions.col

/** Computes an approximation to pi */
object SparkPi {

  def main(args: Array[String]) {
    println("=====================================")
    println("Load Csv into Db, Report statistic")
    println("=====================================")

    val csvFile = args(0)
    val tableName = args(1)
    val dbName = args(2)
    val dbPath = args(3)
    println(s"csvFile=${csvFile}")
    println(s"tableName=${tableName}")
    println(s"dbName=${dbName}")
    println(s"dbPath=${dbPath}")

    val spark = SparkSession
      .builder
      .appName("LoadCsv")
      .getOrCreate()

    println(s"Reading Csv ${csvFile}")
    val dfFile = readCsv(spark, csvFile)

    println(s"Creating Database ${dbName} in path ${dbPath}")
    createDatabase(spark, dbName, dbPath)

    println(s"Persisting data into table ${tableName}")
    // set spark conf to allow overwrite of parquet
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
//    dfFile.write.format("parquet").mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable(s"${dbName}.${tableName}")
    dfFile.write.format("delta").mode(org.apache.spark.sql.SaveMode.Overwrite).saveAsTable(s"${dbName}.${tableName}")

    println("Report summary statistics")
    formatDescribeOutput(dfFile)
    spark.stop()
  }

  def showDescribe(df: DataFrame, colList: List[String]) {
    if (colList.size > 0) {
      val dfSummary = df.select(colList.map(col): _*)
      dfSummary.describe().show()
    }
  }

  def createDatabase(spark:SparkSession, dbname: String, dbpath:String) : Unit = {
    val sql = s"CREATE DATABASE IF NOT EXISTS ${dbname} LOCATION '${dbpath}'"
    println(sql)
    spark.sql(sql)
  }

  def readCsv(spark: SparkSession, csvFilePath: String) : DataFrame = {
    // Read csv files into dataframe using infer schema
    val dfFile = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvFilePath)
    return dfFile
  }

  def formatDescribeOutput(df: DataFrame) {
    // Make it easier to view the column statistics by showing a subset of columns at a time.
    var currentColumns = new ListBuffer[String]()
    val columnList = df.columns
    var i = 0
    val pageSize = 10
    for (columnName <- columnList) {
      i = i + 1
      currentColumns += columnName
      if (i % pageSize == 0) {
        showDescribe(df, currentColumns.toList)
        currentColumns.clear
      }
    }
    // Show the last sets of columns
    if (currentColumns.size > 0) {
      showDescribe(df, currentColumns.toList)
      currentColumns.clear
    }
  }
}