package cdd3.iface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public interface FileReader {
    public Dataset<Row> read(SparkSession sparkSession, Properties properties);
}