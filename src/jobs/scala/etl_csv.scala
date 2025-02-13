import org.apache.spark.sql.SparkSession

object ReadCsvFromS3 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("ReadCsvFromS3")
      .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY_ID")
      .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_ACCESS_KEY")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") // or your S3 endpoint
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
      .getOrCreate()

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true") 
      .load("s3a://testbucket/202301-citibike-tripdata_1.csv")

    df.show()

    spark.stop()
  }
}