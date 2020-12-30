import org.apache.spark.sql.functions.{collect_list, collect_set}
import org.apache.spark.sql.SparkSession


object CollectList extends App{

  val spark = SparkSession.builder().appName("CollectList").master("local[*]").getOrCreate()

  import spark.implicits._

  val arrayDF = Seq(("James","Java"),("James","C#"),("James", "Python"),("Michael","Java"),("Michael","PHP"),("Michael", "PHP"),
    ("Robert", "Java"),("Robert", "Java"),("Robert", "Java"),
    ("Washington", null)).toDF("Name","booksInterestedIn")

  arrayDF.show()

  val df = arrayDF.groupBy("name").agg(collect_list("booksInterestedIn").as("booksInterestedIn"))

  df.show()

  val df2 = arrayDF.groupBy("Name").agg(collect_set("booksInterestedIn")).as("booksInterestedIn")

  df2.show()

}
