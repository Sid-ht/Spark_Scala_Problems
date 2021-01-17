import org.apache.spark.sql.SparkSession

object UDFuse extends App{

  val spark = SparkSession.builder().appName("UDFuse").master("local[*]").getOrCreate()

  val donuts = Seq(("plain donut", 1.50), ("vanilla donut", 2.0), ("glazed donut", 2.50))

  val df = spark.createDataFrame(donuts).toDF("Donut Name", "Price")

  df.show()

  import org.apache.spark.sql.functions._
  import spark.sqlContext.implicits._


  val stockMaxMin : (String => Seq[Int])= donutName => donutName match{
    case "plain donut" => Seq(100,200)
    case "vanilla donut" => Seq(150,250)
    case "glazed donut" => Seq(300,450)
    case _ => Seq(345, 890)

  }

  val udfMaxMin = udf(stockMaxMin)

  val df2 = df.withColumn("Stock Max Min",udfMaxMin($"Donut Name"))

  df2.show()

  def prefixS(s:String): String  = s"s$s"

  val prefix = udf(prefixS _)

  df2.withColumn("Prefix",prefix($"Donut Name")).show()
  
}
