import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, sum}

object GroupBy extends App{

  val spark = SparkSession.builder().appName("GroupBy").master("local[*]").getOrCreate()

  import spark.implicits._

  val goalsDF = Seq(("Messi",4),("Pele",4),("Messi",1),("Messi",3),("Pele",4)).toDF("Player","Goals")

  goalsDF.show()

  goalsDF.groupBy("Player").agg(sum("goals").as("Sum_Goals")).show()


  val studentsDF = Seq(("mario", "italy", "europe"), ("stefano", "italy", "europe"), ("victor", "spain", "europe"),
    ("li", "china", "asia"), ("yuki", "japan", "asia"), ("vito", "italy", "europe")).toDF("name", "country", "continent")


  studentsDF.groupBy("country","continent").agg(count("*")).show()


  val hockeyPlayersDF = Seq(
    ("gretzky", 40, 102, 1990),
    ("gretzky", 41, 122, 1991),
    ("gretzky", 31, 90, 1992),
    ("messier", 33, 61, 1989),
    ("messier", 45, 84, 1991),
    ("messier", 35, 72, 1992),
    ("messier", 25, 66, 1993)
  ).toDF("name", "goals", "assists", "season")

  hockeyPlayersDF.where($"season".isin("1991","1992")).groupBy("name")
    .agg(avg("goals"),avg("assists")).show()

  hockeyPlayersDF.groupBy("name").agg(avg("goals"),
    avg("assists").as("Assisted_Goals")).where($"Assisted_Goals" >= 100).show()
}
