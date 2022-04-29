import org.apache.spark.sql.{SaveMode, SparkSession}

object MySQLConnector extends App {

  System.setProperty("hadoop.home.dir", "C:\\Hadoop")

  val ss = SparkSession.builder()
    .appName(name = "Mon job Spark")
    .master(master = "local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    //.enableHiveSupport()
    .getOrCreate()

  val df_mysql = ss.read
    .format("jdbc")
    .option("url", "jdbc:mysql://127.0.0.1:3306/")
    .option("dbtable", "jea_db.orders")
    .option("user", "consultant")
    .option("password", "pwd#86")
    .option("dbtable", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
    .load()

  df_mysql.show()


  df_mysql.write
    .mode(SaveMode.Overwrite)
    .format(source = "com.databricks.spark.csv")
    .option("header", true)
    .save(path = "C:\\Users\\PLB\\Documents\\Save\\mysql.csv")


  /* df_mysql
    .repartition(1)
    .write
    .partitionBy("city")
    .format("com.databricks.spark.csv")
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .save(path = "C:\\Users\\PLB\\Documents\\Save2\\repartition")

   */

}
