package phi3zh.dataconverter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public abstract class SparkConverter extends BatchConverter<Dataset<Row>> {
    protected SparkSession sparkSession;

    protected SparkConverter(String sparkAppName, String sparkMaster){
        sparkSession = SparkSession.builder().appName(sparkAppName).master(sparkMaster).getOrCreate();
    }

    @Override
    public void run(){
        Dataset<Row> data0 = load();
        Dataset<Row> data = process(data0);
        save(data);
        shutdown();
    }

    protected void shutdown(){}

}
