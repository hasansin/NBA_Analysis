import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._

object BasketballPrediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Basketball Player Prediction")
      .getOrCreate()


    val filePath = "hdfs://namenode:8020/user/hadoop/outputp/part-r-00000"

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
    

  // Step 1: Calculate total points per game for each team
    val teamPoints = cleanedDF.groupBy("GAME_ID", "TEAM_NAME")
      .agg(sum("POINTS_SCORED").as("TEAM_POINTS"))

    // Step 2: Determine winning and losing teams for each game
    val gameResults = teamPoints.groupBy("GAME_ID")
      .agg(
        max(struct(col("TEAM_POINTS"), col("TEAM_NAME"))).as("WINNER"),
        collect_list(struct(col("TEAM_POINTS"), col("TEAM_NAME"))).as("ALL_TEAMS")
      )
      .select(
        col("GAME_ID"),
        col("WINNER.TEAM_NAME").as("WINNING_TEAM"),
        col("WINNER.TEAM_POINTS").as("WINNING_POINTS"),
        expr("filter(ALL_TEAMS, x -> x.TEAM_NAME != WINNER.TEAM_NAME)").as("LOSERS")
      )

    // Flatten losers into individual rows
    val losingTeams = gameResults.selectExpr(
      "GAME_ID",
      "WINNING_TEAM",
      "WINNING_POINTS",
      "explode(LOSERS) as LOSER"
    ).select(
      col("GAME_ID"),
      col("WINNING_TEAM"),
      col("WINNING_POINTS"),
      col("LOSER.TEAM_NAME").as("LOSING_TEAM"),
      col("LOSER.TEAM_POINTS").as("LOSING_POINTS")
    )

    losingTeams.show(truncate = false)

    // Step 3: Join with player data to associate player performance with winning and losing teams
    val playerData = cleanedDF.join(losingTeams, cleanedDF("GAME_ID") === losingTeams("GAME_ID") && cleanedDF("TEAM_NAME") === losingTeams("WINNING_TEAM"), "inner")
    .withColumnRenamed("POINTS_SCORED", "PLAYER_POINTS")
    .withColumnRenamed("TEAM_NAME", "TEAM_NAME_FROM_DATA")
    .drop("GAME_ID")

    playerData.show(truncate = false)


    // Step 4: Prepare features for the ML model
    val assembler = new VectorAssembler()
      .setInputCols(Array("WINNING_POINTS", "LOSING_POINTS", "PLAYER_POINTS"))
      .setOutputCol("features")

    val preparedData = assembler.transform(playerData)
      .withColumn("label", col("PLAYER_POINTS"))

    // Step 5: Split data into training and testing sets
    val Array(trainingData, testData) = preparedData.randomSplit(Array(0.8, 0.2))

    // Step 6: Train the Linear Regression model
    val lr = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.3)

    val lrModel = lr.fit(trainingData)

    // Step 7: Make predictions
    val predictions = lrModel.transform(testData)

    predictions.select("PLAYER_NAME", "WINNING_TEAM", "LOSING_TEAM", "prediction").show(truncate = false)

    // Step 8: Evaluate the model
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    spark.stop()
  }
}
