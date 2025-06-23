import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object q11 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("My Spark Job")
      .master("local[*]")
      .getOrCreate()

    val df1 = spark.read.option("header", "true").csv("C:\\Users\\nkuma309\\OneDrive - Capgemini\\Desktop\\PopulationAnalysis\\FINAL_POPULATION_RURAL.csv")

    val df2 = spark.read.option("header", "true").csv("C:\\Users\\nkuma309\\OneDrive - Capgemini\\Desktop\\PopulationAnalysis\\FINAL_POPULATION_URBAN.csv")

    val df1Alias = df1.alias("left")
    val df2Alias = df2.alias("right")

    val df = df1Alias.join(df2Alias, df1Alias("COUNTRY ID") === df2Alias("COUNTRY ID")).drop(df2Alias("COUNTRY")).drop(df2Alias("COUNTRY ID")).drop(df2Alias("CONTINENT")).drop(df2Alias("DEVELOPMENT_CATEGORY")).drop(df2Alias("GOVERNANCE"))


//    Checking for null values in df
//    val dfWithNulls = df.filter(col("right.D_1950").isNull)
//    dfWithNulls.show()


//    Replacing null values with 0
    val maindf = df.na.fill(0)
//    maindf.show()

    // Cache the DataFrame
    val cachedDF = maindf.cache()

    // Repartition by Governance
    val population = cachedDF.repartition(col("GOVERNANCE"))

//    // Write to disk partitioned by department
//    population.write.mode("overwrite").partitionBy("GOVERNANCE").parquet("C:\\Users\\nkuma309\\OneDrive - Capgemini\\Desktop\\PopulationAnalysis\\Population")
//    population.show()


//    Question 11: Total population rural vs urban

    val r = population.select(col("COUNTRY"), col("left.D_2025").alias("RURAL"), col("right.D_2025").alias("URBAN"))

    val result = r.na.drop()

    result.show()

    result.coalesce(1).write.mode("overwrite").parquet("C:\\Users\\nkuma309\\OneDrive - Capgemini\\Desktop\\SprintResults\\q11.parquet")

  }
}