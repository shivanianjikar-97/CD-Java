package cdd3.driver;

import cdd2.factory.JobFactory;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DriverJob {

    public static void main(String[] args) throws Exception {
        String configFilePath = args[0];
        Properties properties = new Properties();
        properties.load(new java.io.FileReader(configFilePath));
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        JobFactory.getJob(properties, sparkSession).execute();
    }
}
