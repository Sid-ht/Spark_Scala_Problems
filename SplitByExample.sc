import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object SplitByExample extends App{

  val spark = SparkSession.builder().appName("SplitByExample").master("local[*]").getOrCreate()

  val data = Seq(("James, A, Smith","2018","M",3000),("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),("Maria,Anne,Jones","2005","F",4000),("Jen,Mary,Brown","2010","",-1))

  import spark.implicits._

  val df = spark.sparkContext.parallelize(data).toDF("name","dob","gender","salary")

  df.show()

  val split_df = df.select(functions.split(df("name"),",").getItem(0).as("FirstName"),
  functions.split(df("name"),",").getItem(1).as("MiddleName"),
    functions.split(df("name"),",").getItem(2).as("LastName")).drop("name")

  split_df.show(truncate=false)

  val new_df = df.withColumn("FirstName",functions.split(col("name"),",").getItem(0))
    .withColumn("MiddleName", functions.split(col("name"),",").getItem(1))
    .withColumn("LastName",functions.split(col("name"),",").getItem(2)).drop("name")

  new_df.show(truncate=false)
}
