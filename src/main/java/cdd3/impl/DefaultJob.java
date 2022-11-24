package cdd3.impl;

import cdd3.factory.FileReaderFactory;
import cdd3.factory.TableWriterFactory;
import cdd3.iface.IJob;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DefaultJob extends IJob {

    public DefaultJob(Properties properties, SparkSession sparkSession) throws Exception {
        super(properties, sparkSession);
    }

    @Override
    public void execute() {
        //Load file from GCS
        Dataset<Row> dataset = FileReaderFactory.getFileReader(properties).read(sparkSession, properties);

        //transform
        dataset = dataset.selectExpr(properties.getProperty("columnNames"));

        //persist files
        TableWriterFactory.getFileWriter(properties).write(dataset, properties);
    }

}
