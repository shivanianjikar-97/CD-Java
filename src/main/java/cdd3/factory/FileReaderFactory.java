package cdd3.factory;


import cdd3.iface.FileReader;
import cdd3.impl.filereader.*;

import java.util.Properties;

public class FileReaderFactory {
    public static FileReader getFileReader(Properties properties) {
        String source = properties.getProperty("source");
        String sourceFileType = properties.getProperty("sourceFileType");

        if ("hdfs".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new CSVReader();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new PARQUETReader();
        } else if ("GCS".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new GCSCSVReader();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new GCSPARQUETReader();
        } else if ("aws".equalsIgnoreCase(source)) {

            if ("csv".equalsIgnoreCase(sourceFileType))
                return new S3BucketCSVReader();
            else if ("parquet".equalsIgnoreCase(sourceFileType))
                return new S3BucketPARQUETReader();
        }

        return null;
    }
}
