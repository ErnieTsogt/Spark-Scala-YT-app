import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties

class DailyCommentsTest extends AnyFunSuite {

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

  test("Test pobierania danych o komentarzach dla każdego filmu z najpóźniejszą godziną każdego dnia") {
    withSparkSession { spark =>
      val jdbcUrl = "jdbc:mysql://localhost:3307/ytScanDB"
      val jdbcProps = new Properties()
      jdbcProps.setProperty("user", "root")
      jdbcProps.setProperty("password", "zaq12wsx")
      jdbcProps.setProperty("driver", "com.mysql.cj.jdbc.Driver")

      val ytvideosTable = "ytvideos"
      val ytvideosDf: DataFrame = spark.read
        .jdbc(jdbcUrl, ytvideosTable, jdbcProps)

      // Dodaj kolumnę 'scanned_timestamp' i 'comment_day' z formatowaniem daty i godziny
      val dfWithDayAndHour = ytvideosDf
        .withColumn("scanned_timestamp", from_unixtime(col("scanned_date") / 1000).cast("timestamp"))
        .withColumn("comment_day", to_date(col("scanned_timestamp")))
        .withColumn("comment_hour", date_format(col("scanned_timestamp"), "HH"))

      // Ustal ranking na podstawie 'scanned_timestamp' w obrębie każdego dnia
      val windowSpec = Window.partitionBy("google_vid_id", "comment_day")
        .orderBy(col("scanned_timestamp").desc)

      // Dodaj kolumnę 'rank' dla określenia najnowszego wpisu
      val rankedDf = dfWithDayAndHour.withColumn("rank", row_number().over(windowSpec))

      // Filtrowanie, aby zachować tylko najnowsze wpisy (rank == 1) dla każdego dnia i filmu
      val latestHourlyCommentsDf = rankedDf.filter(col("rank") === 1)
        .select("google_vid_id", "comment_day", "comment_hour", "comments")
        .withColumnRenamed("comments", "total_comments")
        .orderBy("google_vid_id", "comment_day")


      latestHourlyCommentsDf.write
        .option("header", "true")
        .mode("overwrite")
        .csv("./daily_comments.csv")




      latestHourlyCommentsDf.show(numRows = Int.MaxValue, truncate = false)
    }
  }
}
