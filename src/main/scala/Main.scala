import org.apache.spark.sql.{SparkSession, DataFrame}
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {
    // Inicjalizacja SparkSession
    val spark = SparkSession.builder()
      .appName("YT-spark-app")
      .master("local[*]") // Można zmienić na inny tryb, np. "yarn" dla Hadoop
      .getOrCreate()

    // URL i właściwości JDBC
    val jdbcUrl = "jdbc:mysql://mysql-db:3306/ytScanDB" // URL do bazy danych MySQL
    val jdbcProps = new Properties()
    jdbcProps.setProperty("user", "****") // Użytkownik
    jdbcProps.setProperty("password", "*******") // Hasło
    jdbcProps.setProperty("driver", "com.mysql.cj.jdbc.Driver") // Sterownik JDBC dla MySQL

    // Wczytywanie danych z tabeli MySQL
    val tableName = "ytvideos" // Nazwa tabeli w MySQL
    val df: DataFrame = spark.read
      .jdbc(jdbcUrl, tableName, jdbcProps)

    // Wyświetlenie schematu i pierwszych kilku wierszy
    df.printSchema()
    df.show(numRows = Int.MaxValue, truncate = false)

  }
}
