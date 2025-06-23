import org.apache.spark.sql.SparkSession
object q4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val urban = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\pankapra\\Desktop\\Sprint\\FINAL_POPULATION_URBAN.csv")
//Ques 4: Urban population of Countries which are Less Developed.
    val result = urban.select(col("COUNTRY"), col("D_2025").alias("Urban_Population"))
      .filter("DEVELOPMENT_CATEGORY = 'LESS DEVELOPED'")
      .na.fill("0")
    result.show()

    result.write.orc("C:\\Users\\pankapra\\Desktop\\Sprint\\ques4")
  }
}
