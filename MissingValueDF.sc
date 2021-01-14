import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object MissingValueDF extends App{

  val spark = SparkSession.builder().appName("MissingValueDF").master("local[*]").getOrCreate()

  val schema = new StructType().add(StructField("DEST_COUNTRY_NAME",StringType,true))
    .add(StructField("ORIGIN_COUNTRY_NAME",StringType,true))
    .add(StructField("count",IntegerType,true))

  val df = spark.read.format("csv").option("header","true").schema(schema).option("delimiter","|").load("src/main/resources/missing_value.csv")

  //df.show()

  df.na.drop(Seq("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME")).show(false)

  val validDF = df.na.fill("unknown destination",Array("DEST_COUNTRY_NAME")).na.fill("unknown source",Array("ORIGIN_COUNTRY_NAME")).na.fill(-1,Array("count"))

  validDF.write.mode("overwrite").option("delimiter","|").option("header","true").csv("/correct_value.csv")

}
