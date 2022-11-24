package cdd1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileReader;
import java.util.Properties;

public class CDD1 {

    public static void main(String[] args) throws Exception {
        String configFilePath = args[0];
        Properties properties = new Properties();
        properties.load(new FileReader(configFilePath));
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();

        String credentialsByte = properties.getProperty("credentials");

        //Load file from GCS
        Dataset<Row> dataset = sparkSession.read().option("credentials", credentialsByte)
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(properties.getProperty("inputFilePath"));

        //transform
        dataset = dataset.selectExpr(properties.getProperty("columnNames"));

        //persist files
        dataset.write().format("bigquery")
                .option("temporaryGcsBucket", properties.getProperty("tempBucketPath"))
                .option("table", properties.getProperty("targetTableName"))
                .option("credentials", credentialsByte)
                .option("project", properties.getProperty("projectId"))
                .save();
    }
}
