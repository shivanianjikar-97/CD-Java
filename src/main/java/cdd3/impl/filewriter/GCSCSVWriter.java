package cdd3.impl.filewriter;

import cdd3.iface.FileWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class GCSCSVWriter implements FileWriter {

    @Override
    public void write(Dataset<Row> dataset, SparkSession sparkSession, Properties properties) {
        dataset.select(properties.getProperty("columnsNames"))
                .write()
                .option("projectId", properties.getProperty("projectId"))
                .option("credentials", properties.getProperty("credentials"))
                .csv(properties.getProperty("targetOutputPath"));
    }
}
