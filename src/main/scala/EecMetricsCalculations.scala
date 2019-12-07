
import Commons._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}

object EecMetricsCalculations {

  def metricsCalculation(config: Config, spark: SparkSession): Unit = {

    val readingsDf = LoadFromMySql(config.databaseName, spark)

    if (!readingsDf.isEmpty) {

      val readingsFormattedDf  = getColumnTypedDf(readingsDf)
      val reagingsByTimeWindow = getGroupByTimeWindow(readingsFormattedDf)

      val outputDf = reagingsByTimeWindow
        .withColumn("energy_consumption", computeEnergyConsumption(col("energyList")))
        .withColumn("mean_power", computeMeanPower(col("powerList")))
        .withColumn("min_power", computeMinPower(col("powerList")))
        .withColumn("max_power", computeMaxPower(col("powerList")))
        .withColumn("first_quartile", computeFirstQuartile(col("powerList")))
        .withColumn("second_quartile", computeSecondQuartile(col("powerList")))
        .withColumn("third_quartile", computeThirdQuartile(col("powerList")))
        .withColumn("audit_computation_time", lit(getLocalTime))
        .select(col("meter"),col("window.start").as("start_time"),
          col("window.end").as("end_time"), col("energy_consumption"),
          col("mean_power"), col("min_power"), col("max_power"),
          col("first_quartile"), col("second_quartile"), col("third_quartile"),
          col("audit_computation_time"))

      writeToMySql(outputDf, config.databaseName, Parameters.computedMetricsTable)
    }
    else {
      println("ERROR: unexpected termination of the program !")
      System.exit(1)
    }
  }

  def LoadFromMySql(database: String, spark: SparkSession): DataFrame = {

    val sqlQuery =
      s"""
         |select * from $database.${Parameters.energyPowerReadingsTable}
         |where audit_integration_time in (
         | select max(audit_integration_time) from $database.${Parameters.energyPowerReadingsTable}
         |)""".stripMargin

    spark.sqlContext
      .read.format("jdbc")
      .option("url", Parameters.mySqlUrl + database)
      .option("driver", Parameters.jdbcDriverName)
      .option("dbtable", s"($sqlQuery) t")
      .option("user", Parameters.mySqlUsername)
      .option("password", Parameters.mySqlPassword)
      .load()
  }

  def getColumnTypedDf(df: DataFrame): DataFrame = {
    df
      .withColumn("time", unix_timestamp(col("time"), Parameters.inputDateTimeFormat).cast(TimestampType))
      .withColumn("energy", df("energy").cast(DoubleType))
      .withColumn("power", df("power").cast(DoubleType))
  }

  def getGroupByTimeWindow(df: DataFrame): DataFrame = {
    df
      .groupBy(col("meter"), window(col("time"), "1 hour"))
      .agg(
        collect_list("energy").as("energyList"),
        collect_list("power").as("powerList"))
  }

  val computeEnergyConsumption: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(arr) => Some(arr.max - arr.min)
    case _    => None
  })

  val computeMeanPower: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).foldLeft(0.0)(_ + _)/(array.length -1))
    case _    => None
  })

  val computeMinPower: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).min)
    case _    => None
  })

  val computeMaxPower: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).max)
    case _    => None
  })

  val computeFirstQuartile: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).foldLeft(0.0)(_ + _)/4)
    case _    => None
  })

  val computeSecondQuartile: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).foldLeft(0.0)(_ + _)/2)
    case _    => None
  })

  val computeThirdQuartile: UserDefinedFunction = udf((array: Seq[Double]) => Option(array) match {
    case Some(_) => Some(array.drop(1).foldLeft(0.0)(_ + _)*3/4)
    case None    => None
  })
}
