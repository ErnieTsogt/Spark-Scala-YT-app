import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class TimestampConversionTest extends AnyFunSuite {

  def withSparkSession(testCode: SparkSession => Any): Unit = {
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .getOrCreate()
    try {
      testCode(spark)
    } finally {
      spark.stop()
    }
  }

  test("Konwersja kolumny scanned_date na timestamp i wyświetlenie bez zapisywania") {
    withSparkSession { spark =>
      val jdbcUrl = "jdbc:mysql://localhost:3307/ytScanDB"
      val jdbcProps = new Properties()
      jdbcProps.setProperty("user", "root") // Użytkownik
      jdbcProps.setProperty("password", "zaq12wsx") // Hasło
      jdbcProps.setProperty("driver", "com.mysql.cj.jdbc.Driver")

      val tableName = "ytvideos"
      try {
        val df: DataFrame = spark.read
          .jdbc(jdbcUrl, tableName, jdbcProps)

        assert(df.schema.fieldNames.contains("scanned_date"), "Kolumna scanned_date nie została znaleziona.")

        df.select(col("scanned_date"), (from_unixtime(col("scanned_date") / 1000).cast("timestamp")).alias("report_day"))
          .show(numRows = Int.MaxValue, truncate = false)

      } catch {
        case e: Exception =>
          fail(s"Nie udało się połączyć z bazą danych lub skonwertować danych: ${e.getMessage}")
      }
    }
  }
}
