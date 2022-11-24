package cdd2.impl;

import cdd2.iface.TableWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class BQWriter implements TableWriter {
    public void write(Dataset<Row> dataset, Properties properties) {
        dataset.write()
                .format("bigquery")
                .option("temporaryGcsBucket", properties.getProperty("tempBucketPath"))
                .option("table", properties.getProperty("targetTableName"))
                .option("credentials", properties.getProperty("credentials"))
                .option("project", properties.getProperty("projectId"))
                .save();
    }
}
