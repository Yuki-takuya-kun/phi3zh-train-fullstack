package phi3zh.dataconverter;

import org.apache.spark.sql.SparkSession;

public abstract class BatchConverter<T> extends Converter<T>{

    @Override
    public void run(){
        T data0 = load();
        T data = process(data0);
        save(data);
    }
}
