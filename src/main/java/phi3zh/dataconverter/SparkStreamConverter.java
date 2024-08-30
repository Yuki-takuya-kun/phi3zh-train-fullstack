package phi3zh.dataconverter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public abstract class SparkStreamConverter extends Converter<Dataset<Row>>{

    protected String bucketName;
    protected SparkSession sparkSession;
    protected transient Config redisConfig;

    protected SparkStreamConverter(Config redisConfig,
                                   String bucketName,
                                   String sparkAppName,
                                   String sparkMaster){
        this.redisConfig = redisConfig;
        this.bucketName = bucketName;
        sparkSession = SparkSession.builder().appName(sparkAppName).master(sparkMaster).getOrCreate();
    }

    @Override
    protected void save(Dataset<Row> data){}

    @Override
    public void run(){
        RedissonClient redissonClient = Redisson.create(redisConfig);
        Dataset<Row> data0 = load();
        Dataset<Row> data1 = process(data0);
        StreamingQuery streamingQuery = streamingSave(data1);
        try {
            while (!(Boolean)redissonClient.getBucket(bucketName).get()){
                Thread.sleep(1000);
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            try {
                streamingQuery.stop();
            } catch (Exception e){
                throw new RuntimeException(e);
            }
            redissonClient.shutdown();
            shutdown();
        }
    }

    protected abstract StreamingQuery streamingSave(Dataset<Row> data);

    /**
     * shutdown all resources
     */
    protected void shutdown(){};
}
