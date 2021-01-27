import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_date, datediff, sum}

object Question extends App {

  val spark = SparkSession.builder().appName("Question").master("local[*]").getOrCreate()

  val df = spark.read.format("csv").option("inferSchema","true").load("src/main/resources/Question.csv")

  val df_new = df.withColumnRenamed("_c0", "id").withColumnRenamed("_c1", "date").withColumnRenamed("_c2", "amount")

  val df_new1  =  df_new.withColumn("datediff",datediff(current_date(),col("date")))

  df_new1.filter(col("datediff") >= 30).groupBy("id").agg(sum("amount").as("Total")).show()

}
