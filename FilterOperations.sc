import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FilterOperations extends App{

  val spark = SparkSession.builder().appName("FilterOperations").master("local[*]").getOrCreate()

  import spark.implicits._

  val df = Seq(("famous amos", true), ("oreo", true), ("ginger snaps", false))
          .toDF("cookie_type", "contains_chocolate")

  df.show()

  val filtered_df = df.where(col("cookie_type") === "oreo")

  filtered_df.show()

  //Fast filtering using partition filter

  val people_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("src/main/resources/people.csv")

  people_df.show()

  people_df.where($"country"=== "Russia" && $"first_name".startsWith("M")).show()

  people_df.repartition($"country").write.option("header","true").partitionBy("country").csv("/Users/siddharth.sinha/Documents/Spark/people_df")

  val new_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Users/siddharth.sinha/Documents/Spark/people_df")

  new_df.where($"country" === "Russia" && $"first_name".startsWith("M")).show()

}
