package ai.prevalent

//importing required packages.
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkJob {

  //To print only error messages.
  Logger.getLogger("org").setLevel(Level.ERROR)

  //Defining the main function.
  def main(args: Array[String]): Unit = {

    //ONLY FOR WINDOWS, TO SET HADOOP_HOME
    System.setProperty("hadoop.home.dir", "file:///C:/winutils/bin/")

    //creating the sparkSession, which acts as the entry point towards the spark.
    val spark = SparkSession
      .builder()
      .appName("SparkJob")
      .master("local[*]")  //This defines the job to run in local and use all the core present in the system.
      .getOrCreate()              //This is done in case if there is already an existing sparkSession present, This will take care of using it.

    //Next, using the sparkSession read the requiredFile(csv) present in local system.(In case of spark-submit, ensure that the file is stored in distributed system like hadoop dfs)
    val ad_DataDF = spark.read
      .option("header", "true")             //To tell the sparkJob to read the header in the csv
      .option("inferSchema", "true")        //This enables sparkJob to inferSchema without explicitly telling the dataFrame
      .csv( "Datasets/sample.csv")

    val windowsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Datasets/windows_EventLogDataSet.csv")

    //Displaying the dataFrame we just created.
    //ad_DataDF.show()

    val joinCondition = ad_DataDF.col("objectSid") === windowsDF.col("objectID")

    val joinedDataFrame = ad_DataDF.join(windowsDF, joinCondition, "inner")
    joinedDataFrame.groupBy("timestamp").agg(count("badPwdCount").alias("Number of badPasswords in each timeStamp")).show()
    joinedDataFrame.select("objectCategory", "timestamp", "impersonationLevel", "badPwdCount").groupBy("impersonationLevel").agg(count("badPwdCount")).show()
    //Stopping the sparkSession
    spark.stop()

  }

}
