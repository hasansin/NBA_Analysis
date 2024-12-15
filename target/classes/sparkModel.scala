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
    

  //  Calculate total points per game for each team
    val teamPoints = cleanedDF.groupBy("GAME_ID", "TEAM_NAME")
      .agg(sum("POINTS_SCORED").as("TEAM_POINTS"))

  //Calculate  the total points per player
    val playerPoints = cleanedDF.groupBy("GAME_ID", "TEAM_NAME","PLAYER_NAME")
      .agg(sum("POINTS_SCORED").as("TOTAl_PLAYER_POINTS"))

    val playerTeamPoints = teamPoints.join(playerPoints, Seq("GAME_ID", "TEAM_NAME"))
    
    playerTeamPoints.show(truncate = false)

    //  Determine winning and losing teams for each game
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
      col("GAME_ID").as("GAMEID"),
      col("WINNING_TEAM"),
      col("WINNING_POINTS"),
      col("LOSER.TEAM_NAME").as("LOSING_TEAM"),
      col("LOSER.TEAM_POINTS").as("LOSING_POINTS")
    )
  
    losingTeams.show(truncate = false)

    // Join with player data to associate player performance with winning and losing teams
    val playerData = playerTeamPoints.join(losingTeams, playerTeamPoints("GAME_ID") === losingTeams("GAMEID") && playerTeamPoints("TEAM_NAME") === losingTeams("WINNING_TEAM"), "inner")
    .withColumnRenamed("TEAM_NAME", "TEAM_NAME_FROM_DATA")
    .withColumn("PLAYER_CONTRIBUTION", col("TOTAl_PLAYER_POINTS") / col("TEAM_POINTS")) // Player's contribution
    .drop("GAMEID")


  // Calculate average player contribution across all games
  val playerAverageContribution = playerData.groupBy("PLAYER_NAME")
  .agg(avg("PLAYER_CONTRIBUTION").as("AVG_PLAYER_CONTRIBUTION"))

   // Join the average player contribution with the original data
    val dataWithAvgContribution = playerData.join(playerAverageContribution, Seq("PLAYER_NAME"));

    dataWithAvgContribution.show(truncate = false)
    
    dataWithAvgContribution.repartition(1)  // Ensures a single partition
    .write
    .option("header", "true")
    .csv("/spark-output/modelInputs");

    // Prepare features for the ML model
    val assembler = new VectorAssembler()
      .setInputCols(Array("WINNING_POINTS", "LOSING_POINTS","AVG_PLAYER_CONTRIBUTION"))
      .setOutputCol("features")

    val preparedData = assembler.transform(dataWithAvgContribution)
      .withColumn("label", col("TOTAl_PLAYER_POINTS"))

    // Split data into training and testing sets
    val Array(trainingData, testData) = preparedData.randomSplit(Array(0.8, 0.2))

    // Train the Linear Regression model
    val lr = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.3)

    val lrModel = lr.fit(trainingData)

    //  Make predictions
    val predictions = lrModel.transform(testData)

    val modelPrediction = predictions.select("PLAYER_NAME", "WINNING_TEAM", "LOSING_TEAM", "prediction")
    
    modelPrediction.show(truncate = false)

    modelPrediction.repartition(1)  // Ensures a single partition
    .write
    .option("header", "true")
    .csv("/spark-output/modelPredictions");

    // Evaluate the model

  // clean the predictions to evaluation
  val cleanedPredictions = predictions
    .filter(col("label").isNotNull && col("prediction").isNotNull) // filtering the null  values n both labels and prediction
    .filter(!col("label").isNaN && !col("prediction").isNaN) // filtering the  NaN values in both labels and prediction
    .filter(!(col("label") === Double.PositiveInfinity || col("label") === Double.NegativeInfinity)) // filtering the infinite values in label
    .filter(!(col("prediction") === Double.PositiveInfinity || col("prediction") === Double.NegativeInfinity)) //  filtering  infinite values in prediction

    // initializing evaluator
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    //evaluate get the rmse value 
    val rmse = evaluator.evaluate(cleanedPredictions)
    println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

    spark.stop()
  }
}
