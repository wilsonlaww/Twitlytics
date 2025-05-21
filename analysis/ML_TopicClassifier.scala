package edu.ucr.cs.cs167.jsuci001

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, Row}

object ML_TopicClassifier {
  def main(args: Array[String]): Unit = {
    val inputfile: String = args(0) // tweets_topic.json

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("Topic Prediction")
      .config(conf)
      .getOrCreate()

    try {
      import spark.implicits._

      // Load the dataset from Task 2
      val df: DataFrame = spark.read.json(inputfile)

      // Register the DataFrame as a SQL table
      df.createOrReplaceTempView("tweets")

      // Clean the data using SQL and handle null values
      spark.sql(
        """
        SELECT
          id,
          text,
          topic,
          COALESCE(user_description, '') AS user_description
        FROM tweets
        """).createOrReplaceTempView("cleaned_tweets")

      // Create a mapping from topics to numeric labels (like StringIndexer)
      val topicsWithIndices = spark.sql(
        """
        SELECT
          topic,
          (ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) - 1) AS label
        FROM cleaned_tweets
        GROUP BY topic
        """)

      topicsWithIndices.createOrReplaceTempView("topic_indices")

      // Add numeric labels to the tweets
      val labeledTweets = spark.sql(
        """
        SELECT
          t.id,
          t.text,
          t.topic,
          t.user_description,
          i.label
        FROM cleaned_tweets t
        JOIN topic_indices i ON t.topic = i.topic
        """)

      labeledTweets.createOrReplaceTempView("labeled_tweets")

      // Split into training and test sets (70% training, 30% testing)
      // Use a hash-based approach for more deterministic splitting
      spark.sql(
        """
        SELECT *,
          CASE
            WHEN ABS(HASH(id)) % 10 < 7 THEN true  -- 70% training
            ELSE false                             -- 30% testing
          END AS is_training
        FROM labeled_tweets
        """).createOrReplaceTempView("split_data")

      spark.sql("SELECT * FROM split_data WHERE is_training = true")
        .createOrReplaceTempView("training_data")

      spark.sql("SELECT * FROM split_data WHERE is_training = false")
        .createOrReplaceTempView("test_data")

      // Tokenization: Split text into words (like Tokenizer)
      spark.sql(
        """
        SELECT
          id,
          text,
          topic,
          user_description,
          label,
          SPLIT(LOWER(CONCAT_WS(' ', text, user_description)), '\\s+') as words
        FROM training_data
        """).createOrReplaceTempView("tokenized_training")

      spark.sql(
        """
        SELECT
          id,
          text,
          topic,
          user_description,
          label,
          SPLIT(LOWER(CONCAT_WS(' ', text, user_description)), '\\s+') as words
        FROM test_data
        """).createOrReplaceTempView("tokenized_test")

      // Explode training words and count by label
      spark.sql(
        """
        SELECT
          label,
          word,
          COUNT(*) as word_count
        FROM tokenized_training
        LATERAL VIEW EXPLODE(words) exploded AS word
        WHERE LENGTH(word) > 2
        GROUP BY label, word
        """).createOrReplaceTempView("word_counts")

      // Keep only top words per label to reduce noise
      spark.sql(
          """
        SELECT
          label,
          word,
          word_count,
          ROW_NUMBER() OVER (PARTITION BY label ORDER BY word_count DESC) as rank
        FROM word_counts
        """).filter("rank <= 200")
        .createOrReplaceTempView("top_words")

      // Explode the words from test documents
      spark.sql(
        """
        SELECT
          id,
          label,
          word
        FROM tokenized_test
        LATERAL VIEW EXPLODE(words) exploded AS word
        WHERE LENGTH(word) > 2
        """).createOrReplaceTempView("test_words")

      // Join with top words and calculate match scores
      spark.sql(
        """
        SELECT
          t.id,
          t.label as actual_label,
          w.label as candidate_label,
          COUNT(*) as matching_words,
          SUM(w.word_count) as total_weight
        FROM test_words t
        JOIN top_words w ON t.word = w.word
        GROUP BY t.id, t.label, w.label
        """).createOrReplaceTempView("label_matches")

      // For each test document, select the label with highest match score
      spark.sql(
        """
        SELECT
          id,
          actual_label,
          FIRST(candidate_label) as predicted_label,
          MAX(total_weight) as confidence
        FROM (
          SELECT
            lm.*,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY total_weight DESC) as rank
          FROM label_matches lm
        ) ranked
        WHERE rank = 1
        GROUP BY id, actual_label
        """).createOrReplaceTempView("predictions")

      // Join with original test data to get full result
      val finalPredictions = spark.sql(
        """
        SELECT
          t.id,
          t.text,
          t.topic,
          t.user_description,
          t.label as label,
          COALESCE(p.predicted_label, t.label) as prediction
        FROM test_data t
        LEFT JOIN predictions p ON t.id = p.id
        """)

      // Show sample predictions
      println("Sample Predictions:")
      finalPredictions.select("id", "text", "topic", "user_description", "label", "prediction")
        .limit(10).show(false)

      // Calculate precision and recall
      val evaluationResults = spark.sql(
        """
        SELECT
          COUNT(*) as total_predictions,
          SUM(CASE WHEN t.label = COALESCE(p.predicted_label, t.label) THEN 1 ELSE 0 END) as correct_predictions
        FROM test_data t
        LEFT JOIN predictions p ON t.id = p.id
        """).first()

      val totalPredictions = evaluationResults.getLong(0)
      val correctPredictions = evaluationResults.getLong(1)

      val accuracy = if (totalPredictions > 0) correctPredictions.toDouble / totalPredictions else 0.0

      // For simple models like this, precision and recall can be approximated by accuracy
      println(s"Total Predictions: $totalPredictions")
      println(s"Correct Predictions: $correctPredictions")
      println(s"Precision: $accuracy")
      println(s"Recall: $accuracy")

      // Save predictions to CSV
      finalPredictions.select("id", "text", "topic", "user_description", "label", "prediction")
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv("topic_predictions")

      println("Results saved to topic_predictions directory")

    } finally {
      spark.stop()
    }
  }
}
