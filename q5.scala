import org.apache.spark.sql.SparkSession
object q5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("My Spark")
      .master("local[*]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val rural = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\pankapra\\Desktop\\Sprint\\FINAL_POPULATION_RURAL.csv")

// Ques 5: Which countries are projected to experience a decline
// in rural population between 1970 and 2025?
    val decRural = rural.select("COUNTRY", "D_1970", "D_2025")
      .filter(col("D_2025") < col("D_1970"))
    decRural.show()

    decRural.write.orc("C:\\Users\\pankapra\\Desktop\\Sprint\\ques5")
  }
}
