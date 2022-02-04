import org.apache.spark.sql.{DataFrame, SparkSession}

object Scala_problem3 {
  def main(args : Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master(master = "local")
      .appName(name = "This is scala problem-3 program")
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "file:////home/sterlite/Spark/spark-events")
      .config("spark.history.fs.logDirectory", "file:////home/sterlite/Spark/spark-events")
      .getOrCreate()

    def readExcel(file: String): DataFrame =sparkSession.read
      .format("com.crealytics.spark.excel")
      .option("location", file)
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("addColorColumns", "False")
      .load()

    val newdf = readExcel("hdfs://hadoop.localhost:9000/Employeedataset/employee_address_details.xlsx")
    newdf.show()


    //val df = sparkSession.read.format(source = "xlsx").option("header", "true")
     // .load(path = "hdfs://hadoop.localhost:9000/Employeedataset/employee_address_details.xlsx")
     //df.show()


    /*val df1 = sparkSession.read.format(source = "xlsx").option("header", "true")
      .load(path = "hdfs://hadoop.localhost:9000/Employeedataset/employee_business_details.xlsx")
    df1.show()

    val df2 = sparkSession.read.format(source = "xlsx").option("header", "true")
      .load(path = "hdfs://hadoop.localhost:9000/Employeedataset/employee_personal_details.xlsx")
    df2.show()

  //  df.createOrReplaceTempView(viewName= "emp_address")
    //df1.createOrReplaceTempView(viewName= "emp_business")
    //df2.createOrReplaceTempView(viewName= "emp_personal")*/




  }

}
