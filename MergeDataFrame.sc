import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, expr, lit}

object MergeDataFrame extends App {

  val spark = SparkSession.builder().appName("MergeDataFrame").master("local[*]").getOrCreate()

  val emp_dataDf1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Users/siddharth.sinha/Documents/Spark/Yelp/Emp_data1.csv")

  val emp_dataDf2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Users/siddharth.sinha/Documents/Spark/Yelp/Emp_data2.csv")

  val missingFields = emp_dataDf1.schema.toSet.diff(emp_dataDf2.schema.toSet)

  var C : DataFrame = null

  for (field <- missingFields){
    C = emp_dataDf2.withColumn(field.name, expr("null"))
  }

  C.union(emp_dataDf1).show()
}
