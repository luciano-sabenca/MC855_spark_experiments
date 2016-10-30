import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.util.Utils
import java.sql.Connection
import java.util.Properties
import java.sql.DriverManager


object Teste {
  def main(args: Array[String]) {
    Class.forName("org.h2.Driver");
    val url = "jdbc:h2:mem:testdb0";
    var conn: java.sql.Connection = null
    val properties = new Properties()
    
    properties.setProperty("user", "testUser");
    properties.setProperty("password", "testPass");
    properties.setProperty("rowId", "false");
    
    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate();
    conn.prepareStatement(
                "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate();
      //val conf = new SparkConf().setMaster("local[2]").setAppName("Simple Application")

      //val sc = new SparkContext(conf)
      
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .enableHiveSupport()
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
      
      
    val password = "testPass"
    val tableName = "tab7"
    val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
    
    val df = spark.sql(
       s"""
       |CREATE TABLE $tableName
       |USING org.apache.spark.sql.jdbc
       |OPTIONS (
       | url '$urlWithUserAndPass',
       | dbtable 'TEST.PEOPLE',
       | user 'testUser',
       | password '$password')
       """.stripMargin).queryExecution.executedPlan

       println(df.toString())
    //val explain = ExplainCommand(df.queryExecution.logical, extended = true)
    //println(explain.toString())
    
    
        /*spark.sessionState.executePlan(explain).executedPlan.executeCollect().foreach { r =>
          println(r.toString)
          assert(!r.toString.contains(password))
        }*/
    } 
}