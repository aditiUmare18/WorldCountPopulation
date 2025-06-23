
import javafx.beans.binding.Bindings.select
import org.apache.spark.sql.SparkSession

object q7{
  def main(args:Array[String]): Unit = {
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

    //Q7:-	Total Population Of a Particular Country in a particular Year

    val result = df.groupBy("COUNTRY")
      .agg(sum(col("r.D_2025")) + sum(col("u.D_2025")) as "Total Population")
      .filter(col("COUNTRY") === "India")

    result.show()
    result.write.orc("C:\\Users\\ridwived\\Downloads\\Rural_POPULATION.csv")

  }
}