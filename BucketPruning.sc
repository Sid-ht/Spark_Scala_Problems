import org.apache.spark.sql.SparkSession


object BucketPruning extends App{

  val spark = SparkSession.builder().appName("Bucket Pruning").config("spark.master","local").getOrCreate()

  val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("src/main/resources/2015-summary.csv")

  df.printSchema()

  df.show()

  df.coalesce(5).write.bucketBy(4,"count").sortBy("count").mode("overwrite").saveAsTable("bucket_demo")

  spark.sql("select * from bucket_demo where origin_country_name = 'Ireland'").show()

  df.coalesce(5).write.mode("overwrite").saveAsTable("unbucketed_demo")

  spark.sql("select * from unbucketed_demo where origin_country_name = 'Ireland'").show()


  spark.stop()

}
