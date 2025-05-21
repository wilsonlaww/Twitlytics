package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object Task5 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: spark-submit Task5 <input_file> <start_date>")
      println("Example: spark-submit Task5 tweets_clean.json 10/21/2017")
      System.exit(1)
    }

    val inputFile: String = args(0)
    val startDate: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    // Create Spark session
    val spark = SparkSession
      .builder()
      .appName("Topic of the Week")
      .config(conf)
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()

    try {
      import spark.implicits._

      // Load the input JSON dataset
      val tweetsDF: DataFrame = spark.read.json(inputFile)
      tweetsDF.createOrReplaceTempView("tweets")

      // Compute the end date
      val dateFormat = new SimpleDateFormat("MM/dd/yyyy")
      val startDateObj = dateFormat.parse(startDate)
      val calendar = Calendar.getInstance()
      calendar.setTime(startDateObj)
      calendar.add(Calendar.DATE, 6)
      val endDateObj = calendar.getTime()
      val endDate = dateFormat.format(endDateObj)

      println(s"Analyzing topics from $startDate to $endDate")

      // Filter tweets with valid country_code and created_at fields
      spark.sql(
        """
          |SELECT *
          |FROM tweets
          |WHERE country_code IS NOT NULL
          |AND created_at IS NOT NULL
          |""".stripMargin).createOrReplaceTempView("tweets_filtered")

      // Extract hashtags and filter tweets within the 7-days
      val filteredTweetsDF = spark.sql(
        s"""
           |WITH exploded_tweets AS (
           |  SELECT
           |    country_code AS Country,
           |    explode(hashtags) AS Topic,
           |    to_timestamp(created_at, 'EEE MMM dd HH:mm:ss Z yyyy') AS tweet_time
           |  FROM
           |    tweets_filtered
           |  WHERE
           |    hashtags IS NOT NULL AND size(hashtags) > 0
           |)
           |
           |SELECT
           |  Country,
           |  Topic AS `Topic-of-the-Week`,
           |  COUNT(*) AS `# of tweets`
           |FROM
           |  exploded_tweets
           |WHERE
           |  tweet_time >= to_date('$startDate', 'MM/dd/yyyy') -- Include start date
           |  AND tweet_time <= to_date('$endDate', 'MM/dd/yyyy') -- Include end date
           |GROUP BY
           |  Country, Topic
           |""".stripMargin)

      filteredTweetsDF.createOrReplaceTempView("all_topics")

      // Find the top 5 trending topics per country
      val topTopicsDF = spark.sql(
        """
          |WITH ranked_topics AS (
          |  SELECT
          |    Country,
          |    `Topic-of-the-Week`,
          |    `# of tweets`,
          |    ROW_NUMBER() OVER (PARTITION BY Country ORDER BY `# of tweets` DESC, `Topic-of-the-Week`) as row_num
          |  FROM
          |    all_topics
          |)
          |
          |SELECT
          |  Country,
          |  `Topic-of-the-Week`,
          |  `# of tweets`
          |FROM
          |  ranked_topics
          |WHERE
          |  row_num <= 5
          |ORDER BY
          |  Country, `# of tweets` DESC, `Topic-of-the-Week`
          |""".stripMargin)

      // Save the results as a CSV file
      topTopicsDF.coalesce(1).write.mode("overwrite").option("header", "true").csv("TopicOfWeek")

      println("Top 5 topics per country for the week:")
      topTopicsDF.show(100, truncate = false)

    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
