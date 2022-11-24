package cdd3.iface;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public interface TableWriter {
    public void write(Dataset<Row> dataset, Properties properties);
}