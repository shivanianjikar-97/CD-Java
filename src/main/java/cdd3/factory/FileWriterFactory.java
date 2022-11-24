package cdd3.factory;


import cdd3.iface.FileWriter;
import cdd3.impl.filewriter.*;

import java.util.Properties;

public class FileWriterFactory {

    public static FileWriter getFileWriter(Properties properties) {
        String source = properties.getProperty("source");
        String sourceFileType = properties.getProperty("sourceFileType");

        if ("hdfs".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new CSVWriter();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new PARQUETWriter();
        } else if ("GCS".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new GCSCSVWriter();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new GCSPARQUETWriter();
        } else if ("aws".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new S3BucketCSVWriter();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new S3BucketPARQUETWriter();
        }

        return null;
    }
}
