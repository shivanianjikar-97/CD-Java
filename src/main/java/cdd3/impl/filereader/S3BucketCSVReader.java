package cdd3.impl.filereader;

import cdd3.iface.FileReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class S3BucketCSVReader implements FileReader {
    public Dataset<Row> read(SparkSession sparkSession, Properties properties) {
        return sparkSession.read()
                .option("credentials", properties.getProperty("credentials"))
                .option("header","true")
                .option("inferSchema", "true")
                .csv(properties.getProperty("inputFilePath"));
    }
}