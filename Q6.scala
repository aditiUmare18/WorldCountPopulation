import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Q6 {
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
      .csv("C:\\Users\\ridwived\\Downloads\\FINAL_POPULATION_RURAL.csv")
      .alias("r")
    val urban = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\ridwived\\Downloads\\FINAL_POPULATION_URBAN.csv")
      .alias("u")

    val df = rural.join(urban, rural("COUNTRY ID") === urban("COUNTRY ID")).drop(urban("COUNTRY")).drop(urban("COUNTRY ID")).drop(urban("CONTINENT")).drop(urban("DEVELOPMENT_CATEGORY")).drop(urban("GOVERNANCE"))

    //Q6:-	Total Population Of a Particular Continent in a particular Year

    val result2 = df.groupBy("CONTINENT")
      .agg(sum(col("r.D_2025")) + sum(col("u.D_2025")) as "Total Population")
      .filter(col("CONTINENT") === "Asia")

    result2.show()
    result2.write.orc("C:\\Users\\ridwived\\Downloads\\Urban_POPULATION.csv")
  }
}
