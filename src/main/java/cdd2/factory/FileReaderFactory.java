package cdd2.factory;


import cdd2.iface.FileReader;
import cdd2.impl.CSVReader;
import cdd2.impl.PARQUETReader;

import java.util.Properties;

public class FileReaderFactory {

    public static FileReader getFileReader(Properties properties) {

        if ("csv".equalsIgnoreCase(properties.getProperty("sourceFileType").toString())) {
            return new CSVReader();
        } else if ("parquet".equalsIgnoreCase(properties.getProperty("sourceFileType").toString())) {
            return new PARQUETReader();
        } else {
            return null;
        }
    }
}
