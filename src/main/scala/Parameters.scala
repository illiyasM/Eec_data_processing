

object Parameters extends Enumeration {

  type Parameters = String

  // Input Data informations
  val inputCsvDelimiter = ","
  val inputDataFilePath = "c:/SquareSense/InputDataFile/"
  val inputDateTimeFormat = "yyyy-MM-dd HH:mm:ss+SS:SS"

  // MySql context properties
  val mySqlUsername = "root"
  val mySqlPassword = "Boo$ter404"
  val mySqlUrl = "jdbc:mysql://localhost:3306/"
  val jdbcDriverName = "com.mysql.jdbc.Driver"

  // Tables
  val energyPowerReadingsTable = "energy_power_readings"
  val computedMetricsTable = "computed_metrics"
}
