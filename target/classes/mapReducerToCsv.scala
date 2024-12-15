import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import spark.implicits._


object DynamicFieldCounter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DynamicFieldCounter")
      .master("local[*]")
      .getOrCreate()


    // File paths for each MapReduce result
    val filePathPlayerScore ="hdfs://namenode:8020/user/hadoop/outputp/part-r-00000";
    val filePathteamScoreScore = "hdfs://namenode:8020/user/hadoop/outputt/part-r-00000";
    val filePathQauterScore =  "hdfs://namenode:8020/user/hadoop/outputq/part-r-00000";
    val filePathMaxScore = "hdfs://namenode:8020/user/hadoop/outputm/part-r-00000";
    

    // Function to process each file
    def processFile(filePath: String,fieldCount :Int) = {
      // Read the file as raw text
      val rawData = spark.read.textFile(filePath)
      
      if(fieldCount == 5){
            val cleanedData = rawData
            .filter(line => line.split(",").length == fieldCount) // Keep only valid lines
            .map { line =>
                val fields = line.split(",").map(_.trim) // Split and trim fields
                (fields(0), fields(1), fields(2), fields(3).toInt, fields(4).toInt) // Convert fields to a tuple
                }
            val cleanedDF = cleanedData.toDF("GAME_ID", "TEAM_NAME", "PLAYER_NAME", "QUARTER", "POINTS_SCORED")
                cleanedDF.show()
                cleanedDF
                    .repartition(1)  // Ensures a single partition
                    .write
                    .option("header", "true")
                    .csv("/spark-output/PlayerScore")   

        }
        else if(fieldCount == 2){
            val cleanedData = rawData.rdd.zipWithIndex()  
                .map { case (text, id) => (id + 1, text) }  
            val cleanedDF = cleanedData.toDF("ID", "TEXT")
            cleanedDF.show()
            cleanedDF
                    .repartition(1)  // Ensures a single partition
                    .write
                    .option("header", "true")
                    .mode("append")
                    .csv("/spark-output/QuarterScoreandMaxScore") 

         } 
         else if(fieldCount == 3){
            val cleanedData = rawData
                .filter(line => line.split("\\s+").length == fieldCount) // Keep only valid lines
                .map { line =>
                    val fields = line.split("\\s+").map(_.trim) // Split and trim fields
                    (fields(0), fields(1).toInt,fields(2).toInt) // Convert fields to a tuple
                    }
            val cleanedDF = cleanedData.toDF("TEAM_NAME", "QUARTER", "TOTAL_POINTS")
            cleanedDF.show()
            cleanedDF
                    .repartition(1)  // Ensures a single partition
                    .write
                    .option("header", "true")
                    .csv("/spark-output/TeamScoreScore") 

        }



    }


    // Process each file and save with headers
      processFile(filePathPlayerScore,5)
      processFile(filePathteamScoreScore,3)
      processFile(filePathQauterScore,2)
      processFile(filePathMaxScore,2)
      
    spark.stop()
  }
}

