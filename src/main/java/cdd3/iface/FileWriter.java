package cdd3.iface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public interface FileWriter {
    public void write(Dataset<Row> dataset, SparkSession sparkSession, Properties properties);
}
