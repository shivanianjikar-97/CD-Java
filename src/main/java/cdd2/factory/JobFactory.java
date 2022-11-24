package cdd2.factory;

import cdd2.iface.IJob;
import cdd2.impl.DefaultJob;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JobFactory {
    public static IJob getJob(Properties properties, SparkSession sparkSession) throws Exception {
        return new DefaultJob(properties, sparkSession);
    }
}
