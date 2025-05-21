#!/usr/bin/env sh
mvn clean package

# TODO: Task3, Task4, Task5

# 1k dataset

spark-submit --class edu.ucr.cs.cs167.jsuci001.PreprocessTweets group_project-1.0-SNAPSHOT.jar Tweets_1k.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task1 group_project-1.0-SNAPSHOT.jar top-hashtags tweets_clean.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task2 group_project-1.0-SNAPSHOT.jar tweets_clean.json

# 10k dataset

spark-submit --class edu.ucr.cs.cs167.jsuci001.PreprocessTweets group_project-1.0-SNAPSHOT.jar Tweets_10k.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task1 group_project-1.0-SNAPSHOT.jar top-hashtags tweets_clean.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task2 group_project-1.0-SNAPSHOT.jar tweets_clean.json

# 100k dataset

spark-submit --class edu.ucr.cs.cs167.jsuci001.PreprocessTweets group_project-1.0-SNAPSHOT.jar Tweets_100k.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task1 group_project-1.0-SNAPSHOT.jar top-hashtags tweets_clean.json

spark-submit --class edu.ucr.cs.cs167.jsuci001.Task2 group_project-1.0-SNAPSHOT.jar tweets_clean.json