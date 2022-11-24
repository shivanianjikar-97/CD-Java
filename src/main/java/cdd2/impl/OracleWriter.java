package cdd2.impl;

import cdd2.iface.TableWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Properties;

public class OracleWriter implements TableWriter {

    @Override
    public void write(Dataset<Row> dataset, Properties properties) {
        dataset.write()
                .format("jdbc")
                .option("url", properties.getProperty("oracleUrl"))
                .option("dbtable", properties.getProperty("oracleUrl"))
                .option("user", properties.getProperty("oracleUsername"))
                .option("password", properties.getProperty("oraclePassword"))
                .save();
    }
}
