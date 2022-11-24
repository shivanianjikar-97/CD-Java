package cddo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CDD0 {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("").getOrCreate();

        //Load file from GCS
        Dataset<Row> dataset = sparkSession.read().option("credentials", "credentials")
                .csv("gs:\\files\\input\\");

        //transform
        dataset = dataset.selectExpr("id,name,address");

        //persist files
        dataset.write().format("bigquery")
                .option("temporaryGcsBucket", "gs:\\files\\input\\")
                .option("table", "DestTable")
                .option("credentials", "credentials")
                .option("project", "projectId")
                .save();
    }
}
