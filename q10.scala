import org.apache.spark.sql.SparkSession

object q10{
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
      .csv("C:\\Users\\pankapra\\Desktop\\Sprint\\FINAL_POPULATION_RURAL.csv")
      .alias("r")
    val urban = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\pankapra\\Desktop\\Sprint\\FINAL_POPULATION_URBAN.csv")
      .alias("u")

    val df = rural.join(urban, rural("COUNTRY ID") === urban("COUNTRY ID"))
      .drop(urban("COUNTRY")).drop(urban("COUNTRY ID"))
      .drop(urban("CONTINENT")).drop(urban("DEVELOPMENT_CATEGORY"))
      .drop(urban("GOVERNANCE"))


    val dfTotal = df.withColumn("Total_2025", col("r.D_2025") + col("u.D_2025"))
    val cont_Totals = dfTotal.groupBy("CONTINENT")
    .agg(sum("Total_2025").alias("Total_2025"))
    val lowest = cont_Totals.orderBy("Total_2025").limit(1)
    lowest.show()
    lowest.write.orc("C:\\Users\\pankapra\\Desktop\\Sprint\\ques10")
  }
}
