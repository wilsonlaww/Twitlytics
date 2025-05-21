package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object TweetsByCountry {
  def main(args: Array[String]): Unit = {
    val inputfile: String = args(0) // tweets_clean.json
    val startDate = args(1) // MM/DD/YYYY
    val endDate = args(2)   // MM/DD/YYYY

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("Tweet Count by Country")
      .config(conf)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY") // Enable old datetime parsing
      .getOrCreate()

    try {
      import spark.implicits._

      // Load tweets dataset
      val df: DataFrame = spark.read.json(inputfile)
      df.createOrReplaceTempView("tweets")

      // Convert `created_at` to timestamp & filter non-null country codes
      val tweetsWithTimestampDF = spark.sql(
        """
          |SELECT country_code,
          |       to_timestamp(created_at, 'EEE MMM dd HH:mm:ss Z yyyy') AS created_at_ts
          |FROM tweets
          |WHERE country_code IS NOT NULL AND created_at IS NOT NULL
          |""".stripMargin)

      tweetsWithTimestampDF.createOrReplaceTempView("tweets_filtered")

      // Convert start and end dates to date format
      val formattedStartDate = s"to_date('$startDate', 'MM/dd/yyyy')"
      val formattedEndDate = s"to_date('$endDate', 'MM/dd/yyyy')"

      // Filter tweets based on the provided date range
      val filteredTweetsDF = spark.sql(
        s"""
           |SELECT country_code, COUNT(*) as count
           |FROM tweets_filtered
           |WHERE to_date(created_at_ts) BETWEEN $formattedStartDate AND $formattedEndDate
           |GROUP BY country_code
           |HAVING count > 50
           |ORDER BY count DESC
           |""".stripMargin)

      // Save output to CSV
      filteredTweetsDF.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("CountryTweetsCount.csv")

      // Show results
      filteredTweetsDF.show()

    } finally {
      spark.stop()
    }
  }
}
