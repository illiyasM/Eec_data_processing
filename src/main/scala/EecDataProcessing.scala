
import org.apache.spark.sql.SparkSession
import EecMetricsCalculations._
import EecReadings.dataFeeding

object EecDataProcessing extends App {

  def Main(config: Config) = {

    val spark = SparkSession.builder
      .master("local")
      .appName("SquareSenseApplication")
      .getOrCreate

    config.action match {
      case "feeding"     => dataFeeding(config, spark)
      case "computation" => metricsCalculation(config, spark)
      case _             => throw new IllegalArgumentException("Wrong operation !")
    }
  }
}
