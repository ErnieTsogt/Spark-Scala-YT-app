import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.Properties

class DatabaseConnectionTest extends AnyFunSuite {

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

  test("Test połączenia z bazą danych MySQL i wczytywania danych") {
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

        assert(df.count() > 0, "Tabela jest pusta, połączenie działa, ale nie ma danych.")

        assert(df.schema.fieldNames.contains("likes"), "Kolumna likes nie została znaleziona.")

        val outContent = new ByteArrayOutputStream()
        Console.withOut(new PrintStream(outContent)) {
          df.show(numRows = Int.MaxValue, truncate = false)
        }

        assert(outContent.toString.contains("ZzFObZq3RTc"), "Nie znaleziono oczekiwanej zawartości w wyjściu.")

      } catch {
        case e: Exception =>
          fail(s"Nie udało się połączyć z bazą danych lub wczytać danych: ${e.getMessage}")
      }
    }
  }
}
