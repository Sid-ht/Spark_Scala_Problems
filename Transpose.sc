import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object Transpose extends App{

  val spark = SparkSession.builder().appName("Transpose").master("local[*]").getOrCreate()

  val df = spark.read.format("csv").option("header","true").option("delimiter","|").load("src/main/resources/marks.csv")

  df.show()

  val new_df = df.groupBy("ID").pivot("Subject").agg(sum("Marks"))

  val columnList = new_df.columns

  val new_columnList = columnList.filter(! _.contains("ID"))

  new_df.withColumn("Total", new_columnList.map(c => col(c)).reduce((c1, c2) => c1 + c2)).show()
}
