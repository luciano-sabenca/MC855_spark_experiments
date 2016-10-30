package segundosemestre.bugAnalysis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.SparkPlan;


public class Bug17783Analysis {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:mem:testdb0";
        Properties properties = new Properties();
        properties.setProperty("user", "testUser");
        properties.setProperty("password", "testPass");
        properties.setProperty("rowId", "false");
        Connection conn = DriverManager.getConnection(url, properties);
        conn.prepareStatement("create schema test").executeUpdate();
        conn.prepareStatement(
                "create table test.people (name TEXT(32) NOT NULL, theid INTEGER NOT NULL)").executeUpdate();
        
        SparkSession spark = SparkSession
                .builder()
                .appName("BugAnalysis")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();
        
        String sql = "CREATE TABLE TAB15 USING org.apache.spark.sql.jdbc OPTIONS (url 'jdbc:h2:mem:testdb0;user=testUser;password=testPass', dbtable 'TEST.PEOPLE',"
                + " user 'testUser', password 'testPass')";
        
        SparkPlan p = spark.sql(sql).queryExecution().executedPlan();
        System.out.println(p.toString());
        
    }
}
