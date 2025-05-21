package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._


import scala.collection.mutable

object PreprocessTweets {
  def main(args: Array[String]): Unit = {
    val inputfile: String = args(0) //  input file name
    val outputfile: String = "tweets" //  output file name

    val getHashtagTexts: UserDefinedFunction = udf((x: mutable.WrappedArray[GenericRowWithSchema]) => {
      x match {
        case x: mutable.WrappedArray[GenericRowWithSchema] => x.map(_.getAs[String]("text"))
        case _ => null
      }
    })

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Lab8 - Preprocessor")
      .config(conf)
      .getOrCreate()
    spark.udf.register("getHashtagTexts", getHashtagTexts)

    try {
      import spark.implicits._
      // Read file and print schema
      val df = spark.read.json(inputfile)
//      df.printSchema()

      // Select relevant columns
      df.createOrReplaceTempView("tweets")
      val selectedDF = spark.sql(
        """
          |SELECT id,
          |       text,
          |       created_at,
          |       place.country_code AS country_code,
          |       getHashtagTexts(entities.hashtags) AS hashtags,
          |       user.description AS user_description,
          |       retweet_count,
          |       reply_count,
          |       quoted_status_id
          |FROM tweets
          |""".stripMargin
      )
      selectedDF.printSchema()

      // Save in JSON format
      selectedDF.write.mode("overwrite").json("tweets_clean.json")

    } finally {
      spark.stop
    }
  }
}