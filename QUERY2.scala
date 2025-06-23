import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object QUERY2{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("World Population Analysis")
      .master("local[*]")
      .getOrCreate()

    // Load CSV files with header and infer schema
    val urbanDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/kchangde/Downloads/FINAL_POPULATION_URBAN 2.csv")

    val ruralDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/kchangde/Downloads/FINAL_POPULATION_RURAL 2.csv")

    // Rename column in urbanDF to match ruralDF
    val urbanRenamed = urbanDF.withColumnRenamed("DEVELOPMENT_CATEGORY", "DEVELOPMENT")

    // List of year columns
    val yearCols = (1950 to 2050 by 5).map(y => col(s"D_$y"))

    // Add TOTAL_POPULATION column to both datasets
    val urbanWithTotal = urbanRenamed.withColumn("TOTAL_POPULATION", yearCols.reduce(_ + _))
    val ruralWithTotal = ruralDF.withColumn("TOTAL_POPULATION", yearCols.reduce(_ + _))

    // Merge datasets
    val mergedDF = urbanWithTotal.unionByName(ruralWithTotal)

    // Country-wise total population
    val countryTotal = mergedDF.groupBy("COUNTRY").agg(sum("TOTAL_POPULATION").alias("TOTAL_POPULATION"))
    println("Country-wise Total Population:")
    countryTotal.show()
    countryTotal.write.mode("overwrite").parquet("C:/Users/kchangde/Desktop/QUES2.parquet")
    spark.stop()
  }
}
