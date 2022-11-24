package cdd2.factory;


import cdd2.iface.TableWriter;
import cdd2.impl.BQWriter;
import cdd2.impl.OracleWriter;

import java.util.Properties;

public class TableWriterFactory {

    public static TableWriter getTableWriter(Properties properties) {
        if ("BQ".equalsIgnoreCase(properties.getProperty("target").toString())) {
            return new BQWriter();
        } else if ("oracle".equalsIgnoreCase(properties.getProperty("target").toString())) {
            return new OracleWriter();
        } else
            return null;
    }
}
