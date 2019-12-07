import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{DataFrame, SaveMode}

object Commons extends App{

  def writeToMySql(dfToPersist: DataFrame, database: String, tableName: String): Unit = {

    val prop = new java.util.Properties()
    prop.put("user", "root")
    prop.put("password", "Boo$ter404")
    prop.put("driver", "com.mysql.jdbc.Driver")

    dfToPersist
      .write
      .mode(SaveMode.Append)
      .jdbc( "jdbc:mysql://localhost:3306/" + database, tableName, prop)
  }

  def getLocalTime: String = {
    DateTimeFormatter.ofPattern(Parameters.inputDateTimeFormat).format(LocalDateTime.now)
  }
}
