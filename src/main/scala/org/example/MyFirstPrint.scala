package org.example
import org.apache.spark.sql.SparkSession


/**
 * Hello world!
 *
 */
object MyFirstPrint extends App {
  println( "Hello World!" )
  System.setProperty("hadoop.home.dir", "C:/Hadoop/")
  val spark = SparkSession.builder.appName("ma_session_spark").getOrCreate()
  val df = spark.read.csv("README.txt")
  df.show()
}


