package com.airlines.analysis
import org.apache.spark._
import java.util.TimeZone
import java.util.Date

object Airlines_data {
  val conf = new SparkConf().setAppName("Airlines_Traffic_Analysis").setMaster("local")
  val sc = new SparkContext(conf)
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  println("SQL Context Intialized")
  def airline_data() = {
  val air_traffic_DF = sqlContext.load("com.databricks.spark.csv", Map("path" -> "C://Users//133288//Desktop//Flight//Air_Traffic_Passenger_Statistics.csv", "header" -> "true"))
  //air_traffic_DF.show()
  air_traffic_DF.registerTempTable("Airports_Traffic")
  
  // Fetching data specific to Airline with Domestic and International Flight Details:
  sqlContext.sql("select GEO_Summary,Operating_Airline, Count(Operating_Airline) from Airports_Traffic GROUP BY GEO_Summary,Operating_Airline ORDER BY Operating_Airline").collect().foreach(println)
  var file_name = "Airline_data"+new Date().getTime()+".csv"
  val Airline_DataDF = sqlContext.sql("select GEO_Summary,Operating_Airline, Count(Operating_Airline) from Airports_Traffic GROUP BY GEO_Summary,Operating_Airline ORDER BY Operating_Airline")
  Airline_DataDF.write.format("com.databricks.spark.csv").save("C://Users//133288//Desktop//Flight//"+file_name)
}
  
  def main(args: Array[String]): Unit = {
   airline_data() 
  }  
}