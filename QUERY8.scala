import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object QUERY8 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Continent-wise Highest Population")
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

    val continentTotal = mergedDF.groupBy("CONTINENT")
      .agg(sum("TOTAL_POPULATION").alias("TOTAL_POPULATION"))
      .withColumn("TOTAL_POPULATION", col("TOTAL_POPULATION").cast(LongType))

    val topContinent = continentTotal.orderBy(desc("TOTAL_POPULATION")).limit(1)
    println("🌍 Continent with Highest Total Population:")
    topContinent.show(false)
    topContinent.write.mode("overwrite").parquet("C:/Users/kchangde/Desktop/QUES8.parquet")
    spark.stop()
  }
}
