import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object QUERY9{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Country-wise Highest Population")
      .master("local[*]")
      .getOrCreate()

    val urbanDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/kchangde/Downloads/FINAL_POPULATION_URBAN 2.csv")

    val ruralDF = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("C:/Users/kchangde/Downloads/FINAL_POPULATION_RURAL 2.csv")

    val urbanRenamed = urbanDF.withColumnRenamed("DEVELOPMENT_CATEGORY", "DEVELOPMENT")
    val yearCols = (1950 to 2050 by 5).map(y => col(s"D_$y"))

    val urbanWithTotal = urbanRenamed.withColumn("TOTAL_POPULATION", yearCols.reduce(_ + _))
    val ruralWithTotal = ruralDF.withColumn("TOTAL_POPULATION", yearCols.reduce(_ + _))
    val mergedDF = urbanWithTotal.unionByName(ruralWithTotal)

    val countryTotal = mergedDF.groupBy("COUNTRY")
      .agg(sum("TOTAL_POPULATION").alias("TOTAL_POPULATION"))
      .withColumn("TOTAL_POPULATION", col("TOTAL_POPULATION").cast(LongType))

    val topCountry = countryTotal.orderBy(desc("TOTAL_POPULATION")).limit(1)
    println("🏆 Country with Highest Total Population:")
    topCountry.show(false)
    topCountry.write.mode("overwrite").parquet("C:/Users/kchangde/Desktop/QUES9.parquet")
    spark.stop()
  }
}
