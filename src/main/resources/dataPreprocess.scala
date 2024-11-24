import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
    .appName("Preprocessing Data")
    .master("spark://spark-master:7077")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()

// Load data from HDFS
val inputData = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("hdfs://namenode:8020/user/hadoop/input/dataset.csv")

inputData.show()
