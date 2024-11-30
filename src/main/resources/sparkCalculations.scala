import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.types._

object NbaAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Basketball Data Cleanup")
      .getOrCreate()

    // File path to the MapReduce output
    val filePath = "hdfs://namenode:8020/user/hadoop/outputp/part-r-00000" // Change to your file path

    // Read the raw output from MapReduce (assuming it's text data with commas separating fields)
    val rawData = spark.read.textFile(filePath)
       // Debug: Print raw data
    println("Raw Data:")
    rawData.take(10).foreach(println)

        // Filter malformed lines
    val validData = rawData.filter(line => line.split(",").length == 5)

    // Debug: Count valid and malformed lines
    val malformedData = rawData.filter(line => line.split(",").length != 5)
    println(s"Total rows: ${rawData.count()}")
    println(s"Valid rows: ${validData.count()}")
    println(s"Malformed rows: ${malformedData.count()}")
    malformedData.collect().foreach(line => println(s"Malformed line: $line"))

    // Clean the data by removing unwanted spaces and splitting by comma
    val cleanedData = rawData
      .filter(line => line.split(",").length == 5) // Keep only valid lines
      .map { line =>
        val fields = line.split(",").map(_.trim) // Split and trim fields
        (fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt) // Convert fields to a tuple
      }

  
    // Convert to DataFrame with column names
    val cleanedDF = cleanedData.toDF("GAME_ID", "TEAM_NAME", "PLAYER_NAME", "QUARTER", "POINTS_SCORED")

    // Show the cleaned DataFrame to inspect
    cleanedDF.show(truncate = false)

    // Perform any further analysis, like calculating percentage of players scoring 40+ points, etc.
    // For example, Task 1: Percentage of players scoring 40+ points
    val playerScores = cleanedDF.groupBy("GAME_ID", "PLAYER_NAME")
      .agg(sum("POINTS_SCORED").as("TOTAL_POINTS"))

    val playersWith40Plus = playerScores.filter(col("TOTAL_POINTS") >= 40)

    val totalPlayers = playerScores.select("PLAYER_NAME").distinct().count()
    val total40PlusPlayers = playersWith40Plus.count()
    val percentage = (total40PlusPlayers.toDouble / totalPlayers) * 100
    println(f"Percentage of players scoring 40+ points: $percentage%.2f%%")

    // Aggregate total points scored by each team in each match
    val teamScores = cleanedDF.groupBy("GAME_ID", "TEAM_NAME")
      .agg(sum("POINTS_SCORED").as("TOTAL_POINTS"))


    // Identify winners and losers for each match
    val matchResults = teamScores.groupBy("GAME_ID")
      .agg(
        max(struct(col("TOTAL_POINTS"), col("TEAM_NAME"))).as("WINNER"), // Team with the highest score
        collect_list(struct(col("TOTAL_POINTS"), col("TEAM_NAME"))).as("ALL_TEAMS")
      )
      .select(
        col("GAME_ID"),
        col("WINNER.TEAM_NAME").as("WINNING_TEAM"),
        col("ALL_TEAMS").as("ALL_TEAMS")
      )

    // Extract losers from ALL_TEAMS (all teams except the winner)
    val losers = matchResults.withColumn("LOSING_TEAMS", expr(
      "filter(ALL_TEAMS, x -> x.TEAM_NAME != WINNING_TEAM)"
    ))
      .select("GAME_ID", "WINNING_TEAM", "LOSING_TEAMS")

    // Flatten losers into individual rows
    val flattenedLosers = losers.selectExpr(
      "GAME_ID",
      "explode(LOSING_TEAMS) as LOSER"
    )
      .select(
        col("GAME_ID"),
        col("LOSER.TEAM_NAME").as("LOSING_TEAM")
      )

    // Count the number of matches lost by each team
    val matchesLost = flattenedLosers.groupBy("LOSING_TEAM")
      .count()
      .withColumnRenamed("count", "MATCHES_LOST")

    // Show the results
    matchesLost.show(truncate = false)


    // Stop Spark session
    spark.stop()
  }
}
