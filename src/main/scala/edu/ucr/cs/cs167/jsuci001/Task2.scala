package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Task2 {
  def main(args: Array[String]): Unit = {
    val inputfile: String = args(0) // tweets_clean.json

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("Twitter Topic Assignment")
      .config(conf)
      .getOrCreate()

    try {
      import spark.implicits._
      val df: DataFrame = spark.read.json(inputfile)
      df.createOrReplaceTempView("tweets")

      // Task1 does not save top 20 hashtags, recompute here
      val topHashtagsDF = spark.sql(
        """
          |SELECT hashtag, COUNT(*) as count
          |FROM (SELECT EXPLODE(hashtags) as hashtag FROM tweets)
          |GROUP BY hashtag
          |ORDER BY count DESC
          |LIMIT 20
          |""".stripMargin)

      val topHashtags = topHashtagsDF.select("hashtag").as[String].collect()

      // Assign topic using array_intersect, keep the first match
      val tweetsWithTopicDF = spark.sql(
        s"""
           |SELECT id, text, created_at, country_code,
           |       array_intersect(hashtags, array(${topHashtags.map(h => s"'$h'").mkString(",")}))[0] as topic,
           |       user_description, retweet_count, reply_count, quoted_status_id
           |FROM tweets
           |WHERE size(array_intersect(hashtags, array(${topHashtags.map(h => s"'$h'").mkString(",")}))) > 0
           |""".stripMargin)

      // Save output to tweets_topic.json
      tweetsWithTopicDF.write.mode("overwrite").json("tweets_topic.json")

      // Check schema
      tweetsWithTopicDF.printSchema()

      // Print total count
      println(s"Total records in tweets_topic: ${tweetsWithTopicDF.count()}")

    } finally {
      spark.stop()
    }
  }
}
