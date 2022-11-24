package cdd2.iface;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public abstract class IJob {
    protected Properties properties;
    protected SparkSession sparkSession;

    public IJob(Properties properties, SparkSession sparkSession) throws Exception {
        this.properties = properties;
        this.sparkSession = sparkSession;
    }

    abstract public void execute();
}
