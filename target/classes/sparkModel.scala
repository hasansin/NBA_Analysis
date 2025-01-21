//import required values
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._


object BasketballPrediction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Basketball Player Prediction")
      .getOrCreate()

    //get file path to get data from hadoop map redeucer output
    val filePath = "hdfs://namenode:8020/user/hadoop/outputp/part-r-00000"

    // read data from above path
    val rawData = spark.read.textFile(filePath)
    // Debug: Print raw data
    println("Raw Data:")
    rawData.take(10).foreach(println)

    // filter and see whether malformed lines are there
    val validData = rawData.filter(line => line.split(",").length == 5)

    // Debug: Count valid and malformed lines
    val malformedData = rawData.filter(line => line.split(",").length != 5)
    println(s"Total rows: ${rawData.count()}")
    println(s"Valid rows: ${validData.count()}")
    println(s"Malformed rows: ${malformedData.count()}")
    malformedData.collect().foreach(line => println(s"Malformed line: $line"))

    // Clean the data by removing unwanted spaces and then splitting data by comma to create the dataset
    val cleanedData = rawData
      .filter(line => line.split(",").length == 5) // Keep only valid lines
      .map { line =>
        val fields = line.split(",").map(_.trim) // Split and trim fields
        (fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt) // Convert fields to a tuple
      }

  
    // add headers to the data frame
    val cleanedDF = cleanedData.toDF("GAME_ID", "TEAM_NAME", "PLAYER_NAME", "QUARTER", "POINTS_SCORED")

    //display data set
    cleanedDF.show(truncate = false)
    

  //  calculate the totala points scored by each team
    val teamPoints = cleanedDF.groupBy("GAME_ID", "TEAM_NAME")
      .agg(sum("POINTS_SCORED").as("TEAM_POINTS"))

  // calculate the total point scored by each player
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
    val linearRegression = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(100)
      .setRegParam(0.3)

    val decisionTree = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(5)

    // Cross-Validation for Linear Regression
    val paramGridLR = new ParamGridBuilder()
      .addGrid(linearRegression.regParam, Array(0.01, 0.1, 0.3))
      .addGrid(linearRegression.maxIter, Array(50, 100))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val crossValidatorLR = new CrossValidator()
      .setEstimator(linearRegression) // model tobe tranined
      .setEvaluator(evaluator) // metric to evaluate the model
      .setEstimatorParamMaps(paramGridLR) // hyper parameter combination to test
      .setNumFolds(5) // number of folds in cross validation

    val cvModelLR = crossValidatorLR.fit(trainingData)

    // Evaluate Linear Regression
    val predictionsLR = cvModelLR.transform(testData)
    val rmseLR = evaluator.evaluate(predictionsLR)
    println(s"Linear Regression RMSE: $rmseLR")

    // Cross-Validation for Decision Tree
    val paramGridDT = new ParamGridBuilder()
      .addGrid(decisionTree.maxDepth, Array(3, 5, 10))
      .addGrid(decisionTree.maxBins, Array(16, 32, 64))
      .build()

    val crossValidatorDT = new CrossValidator()
      .setEstimator(decisionTree) // model tobe tranined
      .setEvaluator(evaluator) // metric to evaluate the model
      .setEstimatorParamMaps(paramGridDT) // hyper parameter combination to test
      .setNumFolds(5) // number of folds in cross validation

    val cvModelDT = crossValidatorDT.fit(trainingData)

    // Evaluate Decision Tree
    val predictionsDT = cvModelDT.transform(testData)
    val rmseDT = evaluator.evaluate(predictionsDT)
    println(s"Decision Tree RMSE: $rmseDT")

    // Select the Best Model
    val (bestModel, bestModelType) = if (rmseLR < rmseDT) {
      println("Linear Regression is the better model.")
      (cvModelLR, "Linear Regression")
    } else {
      println("Decision Tree is the better model.")
      (cvModelDT, "Decision Tree")
    }

    //  Use the Best Model for Predictions
    val predictions = bestModel.transform(testData)

    val PredictedData = predictions.select("PLAYER_NAME", "WINNING_TEAM","LOSING_TEAM", "label", "prediction")
    preparedData.show(truncate = false)

    PredictedData.repartition(1)  // Ensures a single partition
    .write
    .option("header", "true")
    .csv("/spark-output/PredictedData");
    spark.stop()
  }
}
