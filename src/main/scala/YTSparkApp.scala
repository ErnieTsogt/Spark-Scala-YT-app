import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Properties
//    --add-exports=java.base/sun.nio.ch=ALL-UNNAMED

object YTSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("YT-spark-app")
      .master("local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:mysql://localhost:3307/ytScanDB"
    val jdbcProps = new Properties()
    jdbcProps.setProperty("user", "root")
    jdbcProps.setProperty("password", "zaq12wsx")
    jdbcProps.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val tableName = "ytvideos"
    val df: DataFrame = spark.read
      .jdbc(jdbcUrl, tableName, jdbcProps)


    val dfWithDayAndHour = df
      .withColumn("scanned_timestamp", from_unixtime(col("scanned_date") / 1000).cast("timestamp"))
      .withColumn("report_date", to_date(col("scanned_timestamp")))
      .withColumn("comment_hour", date_format(col("scanned_timestamp"), "HH"))

//    dfWithDayAndHour.groupBy("scanned_timestamp", ).count()

    val windowSpec = Window.partitionBy("google_vid_id", "report_date")
      .orderBy(col("scanned_timestamp").desc)

    val rankedDf = dfWithDayAndHour.withColumn("rank", row_number().over(windowSpec))

    val latestHourlyCommentsDf = rankedDf.filter(col("rank") === 1)
      .select("google_vid_id", "report_date", "comment_hour", "comments")
      .withColumnRenamed("comments", "total_comments")
      .orderBy("google_vid_id", "report_date")

    latestHourlyCommentsDf.write
      .option("header", "true")
      .mode("overwrite")
      .csv("daily_comments.csv")

    //   "--add-opens java.base/sun.util.calendar=ALL-UNNAMED"
      latestHourlyCommentsDf
        .select("google_vid_id", "total_comments", "report_date")
        .write
        .mode("append")
        .jdbc(jdbcUrl, "daily_comments", jdbcProps)


    latestHourlyCommentsDf.show(numRows = Int.MaxValue, truncate = false)

    spark.stop()
  }
}
