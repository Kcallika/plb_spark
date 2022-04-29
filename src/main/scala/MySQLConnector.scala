import org.apache.spark.sql.SparkSession

object MySQLConnector extends App {

  System.setProperty("hadoop.home.dir", "C:\\PBL\\Hadoop")

  val ss = SparkSession.builder()
    .appName(name = "Mon job Spark")
    .master(master = "local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    //.enableHiveSupport()
    .getOrCreate()

  val df_mysql = ss.read
    .format("jdbc")
    .option("url", "jdbc:mysql://127.0.0.1:3306/jea_db")
    .option("dbtable", "jea_db.orders")
    .option("user", "consultant")
    .option("password", "pwd#86")
    .option("query", "(select state, city, sum(round(numunits * totalprice)) as commandes_totales from jea_db.orders group by state, city) requete")
    .load()

  df_mysql.show()

}
