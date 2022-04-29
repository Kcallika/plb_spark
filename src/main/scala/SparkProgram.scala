import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, lit, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
// import org.apache.spark.serializer.KryoSerializer

// we need to extend existing object
object SparkProgram extends App {

  System.setProperty("hadoop.home.dir", "C:/hadoop/")

  val ss = SparkSession.builder()

    .appName(name = "my Job Spark")
    .master("local[*]") // localhost + nb cpu (?)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    //.enableHiveSupport()  // not installed
    .getOrCreate()


  // create RDD
  val list1 : List[Int] = List(1,8, 5, 6, 9, 59)
  val list_names : List[String] = List("Oxana", "Alex", "Valentin")

  val rdd_1 : RDD[String] = ss.sparkContext.parallelize(List("Oxana", "Alex", "Valentin"))
  val rdd_2 = ss.sparkContext.parallelize(list1)

  // action
  rdd_1.foreach(e => println(e))
  rdd_1.count()

  // transform
  val rdd_trans = rdd_1.map(e => e.length)
  rdd_trans.foreach(e => println(e))

  // file
  val rdd_file = ss.sparkContext.textFile(path = "C:\\Users\\PLB\\Documents\\Data\\data\\csv")
  rdd_file.foreach(e => println(e))

  val df_csv = ss.read
    .format(source="csv")
    .option("sep", ",")
    .option("inferSchema", "true")  // guess my file schema: nb of columns, etc.
    .option("header", "true")
    .load(path="C:\\Users\\PLB\\Documents\\Data\\data\\csv")

  df_csv.show(numRows = 30)

  val df_json = ss.read
    .format(source="json")
    //.option("sep", ",")
    //.option("inferSchema", "true")  // guess my file schema: nb of columns, etc.
    //.option("header", "true")
    .load(path="C:\\Users\\PLB\\Documents\\Data\\data\\json")

  df_json.show(numRows = 10)

  val df_parquet = ss.read
    .format(source="parquet")
    //.option("sep", ",")
    //.option("inferSchema", "true")  // guess my file schema: nb of columns, etc.
    //.option("header", "true")
    .load(path="C:\\Users\\PLB\\Documents\\Data\\data\\parquet\\2010-summary.parquet")

  df_parquet.show(numRows = 10)

  // operations
  println("=========== CSV SCHEMA ==============")
  df_csv.printSchema()

  // add new column ventes, concatenation and conditional column vente_cat
  println("=========== NEW CSV  ==============")
  val df_csv_2 = df_csv.select(
    col(colName = "DEST_COUNTRY_NAME"),
    col(colName = "count").cast(LongType)
  ).withColumn(colName = "ventes", col(colName = "count") + lit(10))  // lit for literal func
    .withColumn("concatenation", concat(col(colName = "count"), col(colName = "DEST_COUNTRY_NAME")))
    .withColumn("vente_categorie", when(col(colName = "count") < 200, lit("Bad") ).otherwise(
      when(col(colName = "count").between(200, 400), lit("Average")).otherwise(
        when(col(colName = "count") > 400, lit("Bravo"))
      )
    ))

  df_csv_2.show(numRows = 15)

  // filter
  df_csv_2.filter(col(colName = "DEST_COUNTRY_NAME") === lit("Egypt") &&
    col(colName = "vente_categorie").isin("Bad", "Bravo")||
    col(colName = "ventes") > 1000 )
    .show(20)

  // grouby
  df_csv_2.filter(col(colName = "DEST_COUNTRY_NAME") === lit("Egypt") &&
    col(colName = "vente_categorie").isin("Bad", "Bravo")||
    col(colName = "ventes") > 1000 )
    .groupBy(col(colName = "DEST_COUNTRY_NAME"))
    .count()
    .show(20)

  df_csv_2.groupBy(col(colName = "DEST_COUNTRY_NAME"))
    .max("count").alias(alias = "Max vente")  // as for afficahge
    .show()


  // strict type
  // print schemas as before and then declare it
  val schema_order_line = StructType(
    List(
      StructField("orderlineid", IntegerType, false),
      StructField("orderid", IntegerType, true),
      StructField("productid", IntegerType, true),
      StructField("shipdate", TimestampType, true),
      StructField("billdate", TimestampType, true),
      StructField("unitprice", DoubleType, true),
      StructField("numunits", IntegerType, true),
      StructField("totalprice", DoubleType, true)
    )
  )

  val schema_orders = StructType(
    List(
      StructField("orderid", IntegerType, false),
      StructField("customerid", IntegerType, true),
      StructField("campaignid", IntegerType, true),
      StructField("orderdate", TimestampType, true),
      StructField("city", StringType, true),
      StructField("state", StringType, true),
      StructField("zipcode", StringType, true),
      StructField("paymenttype", StringType, true),
      StructField("totalprice", DoubleType, true),
      StructField("numorderlines", IntegerType, true),
      StructField("numunits", IntegerType, true)
    )
  )


  // join data from order and orderline files
  println("=========== ORDERS  ==============")
  val df_orders = ss.read
    .format(source="csv")
    .schema(schema_orders)
    .option("sep", "\t")
    .load(path="C:\\Users\\PLB\\Documents\\Data\\data\\orders.txt")

  df_orders.printSchema()
  df_orders.show(15)

  println(" ==================== ORDER LINE ===============")
  val df_orderline = ss.read
    .format(source="csv")
    .schema(schema_order_line)
    .option("sep", "\t")
    .option("inferSchema", "true")  // guess my file schema: nb of columns, etc.
    .option("header", "true")
    .load(path="C:\\Users\\PLB\\Documents\\Data\\data\\orderline.txt")

  df_orderline.printSchema()
  df_orderline.show(15)

  // real joint starts here
  // right: big table, left: small table
  // join small with big
  println("===================== JOIN ===================")
  val df_join = df_orders.join(df_orderline, df_orders("orderid") === df_orderline("orderlineid"), joinType = "inner" )
   // .select(col("instruction here"))  // to avoid identical names

  df_join.printSchema()
  df_join.show(10)

  // it a new day
  // create table in global catalog spark
  // like pointer
  println("================ SQL ======================== ")
  df_orderline.createOrReplaceTempView(viewName = "table_detailsCommandes")
  df_orders.createOrReplaceTempView("tables_commandes")
  val df_sql = ss.sql(sqlText= "SELECT * FROM table_detailsCommandes dc INNER JOIN tables_commandes  tc ON dc.orderid = tc.orderid LIMIT 5")
  df_sql.show()

  // info about query
  df_sql.explain()



}
