import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

import scala.collection.mutable.ListBuffer

object ListOperations extends App{

  val spark = SparkSession.builder().appName("ListOperations").master("local[*]").getOrCreate()

  val list = List("ABH101BK40","KBH101KJ60","XXH101YU30","HBV102VB20","IOV102BU30")

  val ids = new ListBuffer[String]()
  val values = new ListBuffer[String]()


  for ( i <- list){
    ids += i.slice(3,6)
    values += i.slice(8,10)
  }

  println(ids)
  println(values)

  import spark.implicits._

  var new_list = ListBuffer[(String, String)]()
  new_list = ids zip values


  val df = spark.sparkContext.parallelize(new_list).toDF("ids","values")

  df.show()

  df.groupBy("ids").agg(sum("values").as("Sum")).show()


}
