
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import Commons._

object EecReadings extends App {

  def dataFeeding(config: Config, spark: SparkSession): Unit = {

    val inputFilePath = Parameters.inputDataFilePath + config.inputFileName.getOrElse("")

    val csvDf = loadCsvFile(inputFilePath, config.inputCsvHeader.getOrElse(true), spark)
        .withColumn("audit_integration_time", lit(getLocalTime))
        .withColumn("audit_input_file_name", lit(config.inputFileName.getOrElse("")))

    if (!csvDf.isEmpty) {
      writeToMySql(csvDf, config.databaseName, Parameters.energyPowerReadingsTable)
    }
    else {
      println(s" No data has been integrated !")
    }
  }

  def loadCsvFile(inputFilePath: String, inputCsvHeader: Boolean, spark: SparkSession): DataFrame = {
    spark
      .read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
      .option("header", inputCsvHeader)
      .option("delimiter", Parameters.inputCsvDelimiter)
      .load(inputFilePath)
  }
}

