
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object main {
  def main(args:Array[String]):Unit = {

  //  Development Wise Total Population
  val spark = SparkSession.builder().appName("My Spark").master("local[*]").getOrCreate()

    val urbanDF = spark.read.option("header" , "true").csv("C:\\Users\\ajaypalu\\Downloads\\FINAL_POPULATION_URBAN 3.csv")
      .withColumnRenamed("D_2025" , "Urban_2025")

    val ruralDF = spark.read.option("header" , "true").csv("C:\\Users\\ajaypalu\\Downloads\\FINAL_POPULATION_RURAL 3.csv")
      .withColumnRenamed("D_2025" , "Rural_2025")

    val joinDF = urbanDF.join(ruralDF , Seq("Country ID"), "inner")

    val updatedDF = joinDF.withColumn("Total_2025" , col("Urban_2025") + col("Rural_2025"))

    val result3 = updatedDF.select("DEVELOPMENT" , "Total_2025").groupBy("DEVELOPMENT")
     .agg(sum("Total_2025").as("TotalDevelopmentOf_2025"))

    result3.show()

    result3.write.option("header" , "true").csv("C:\\Users\\ajaypalu\\OneDrive - Capgemini\\" +
      "Desktop\\developmentWise.csv")
  }
}

