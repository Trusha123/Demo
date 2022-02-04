/* @author trusha.patel@stltech.in
* @ version 2.0
* @ Date 02-Feb-2022
* @ Copyright Sterlite Technologies Ltd. All Rights reserved.
* @ Description: To demonstrate the csv file to display.
* */
package HDFSFileFormat
import org.apache.spark.sql.SparkSession

object HdfsFileFormat {
  def main(args : Array[String]): Unit =
  {
    val sparkSession = SparkSession.builder()
      .master(master = "local")
      .appName(name = "This is first scala program")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    val df = sparkSession.read.format(source = "csv").option("header", "true")
      .load(path = "hdfs://hadoop.localhost:9000/test/employee_address_details.csv")
    df.show()

    //store displayed records in HDFS
 //  df.write.csv(path = "hdfs://hadoop.localhost:9000/test/Display_record")


    //To filter by regions
    //val df1 = df.filter(df("Region")==="Northeast")
    //df1.show()
   // df1.write.csv(path = "hdfs://hadoop.localhost:9000/test/New_Northeast")
    //val df2 = df.filter(df("Region")==="South")
    //df2.show()
    //df1.write.csv(path = "hdfs://hadoop.localhost:9000/test/New_South")
    val df3 = df.filter(df("Region")==="Midwest")
    //df3.show()
    //df1.write.csv(path = "hdfs://hadoop.localhost:9000/test/New_Midwest")
    val df4 = df.filter(df("Region")==="West")
    //df4.show()
    //df1.write.csv(path = "hdfs://hadoop.localhost:9000/test/New_West")
    val df5 = df.filter(df("Region")==="East")
    //df5.show()
    //df1.write.csv(path = "hdfs://hadoop.localhost:9000/test/New_East")

    //df.write.partitionBy(colNames = "Region").csv(path = "hdfs://hadoop.localhost:9000/test/Newdataset")

    //df.select("Region").show()
    //df.select("Region").collect()
    //df.registerTempTable("Demo_Employee")
    //df.createOrReplaceTempView("Demo_employee")
  }

}
