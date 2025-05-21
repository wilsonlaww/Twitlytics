package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CleanTweets {

  def main(args: Array[String]) {
    val operation: String = args(0)
    val inputfile: String = args(1)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("Twitter Data Analysis")
      .config(conf)
      .getOrCreate()

    if (!(inputfile.endsWith(".json") || inputfile.endsWith(".parquet"))) {
      Console.err.println(s"Unexpected input format. Expected file name to end with either '.json' or '.parquet'.")
      System.exit(1)
    }

    var df: DataFrame = if (inputfile.endsWith(".json")) {
      spark.read.json(inputfile)
    } else {
      spark.read.parquet(inputfile)
    }

    df.createOrReplaceTempView("tweets")

    try {
      import spark.implicits._
      var valid_operation = true
      val t1 = System.nanoTime

      operation match {
        case "top-hashtags" =>
          // Explode the hashtags column to create separate rows per hashtag
          val explodedDF = df.withColumn("hashtag", explode(col("hashtags")))

          // Create a view for the new dataframe
          explodedDF.createOrReplaceTempView("hashtags_view")

          // Use SQL to get the top 20 hashtags with the most tweets
          val topHashtagsDF = spark.sql(
            """
              |SELECT hashtag, COUNT(*) as count
              |FROM hashtags_view
              |GROUP BY hashtag
              |ORDER BY count DESC
              |LIMIT 20
              |""".stripMargin)

          // Show the final result
          topHashtagsDF.show(20, truncate = false)

          // Print the hashtags as a comma-separated list
          val topHashtagsList = topHashtagsDF.collect().map(_.getString(0)).mkString(", ")
          println(s"Top 20 hashtags: $topHashtagsList")

        case _ =>
          valid_operation = false
      }

      val t2 = System.nanoTime
      if (valid_operation)
        println(s"Operation $operation on file '$inputfile' finished in ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")

    } finally {
      spark.stop()
    }
  }
}
