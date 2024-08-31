package phi3zh.dataconverter;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.RedissonClientFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class SparkKafkaConverter extends SparkStreamConverter{
    protected String monitorSemaphore;
    protected String kafkaServer;
    protected String topicName;
    protected String startingOffset;
    protected String redisServer;
    protected AtomicInteger counter = new AtomicInteger(0);
    protected transient StreamingQuery monitorQuery;

    protected SparkKafkaConverter(Config redisConfig,
                                  String bucketName,
                                  String sparkAppName,
                                  String sparkMaster,
                                  String kafkaServer,
                                  String topicName,
                                  String startingOffset,
                                  String monitorSemaphore){
        super(redisConfig, bucketName, sparkAppName, sparkMaster);
        Properties kafkaClientConfig = new Properties();
        kafkaClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        this.redisServer = redisConfig.useSingleServer().getAddress();
        this.kafkaServer = kafkaServer;
        this.topicName = topicName;
        this.monitorSemaphore = monitorSemaphore;
        this.startingOffset = startingOffset;
    }

    protected SparkKafkaConverter(Config redisConfig,
                                  String bucketName,
                                  String sparkAppName,
                                  String sparkMaster,
                                  String kafkaServer,
                                  String topicName,
                                  String startingOffset){
        super(redisConfig, bucketName, sparkAppName, sparkMaster);
        Properties kafkaClientConfig = new Properties();
        kafkaClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        this.redisServer = redisConfig.useSingleServer().getAddress();
        this.kafkaServer = kafkaServer;
        this.topicName = topicName;
        this.startingOffset = startingOffset;
    }

    @Override
    protected Dataset<Row> load(){
        Dataset<Row> rowDataset =  sparkSession.readStream().format("kafka")
                .option("kafka.bootstrap.servers", this.kafkaServer)
                .option("subscribe", this.topicName)
                .option("startingOffsets", this.startingOffset)
                .load().filter("key is not null and value is not null");
        Dataset<Row> result = rowDataset.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value");
        return result;
    }

    @Override
    public void run(){
        RedissonClient redissonClient = RedissonClientFactory.getInstance(redisConfig);
        Dataset<Row> data0 = load();
        if (this.monitorSemaphore != null){
            monitorQuery = preprocess(data0);
        }
        Dataset<Row> data1 = process(data0);
        StreamingQuery streamingQuery = streamingSave(data1);
        try {
            System.out.println("the bucket name:" + bucketName);
            while (!((Boolean)(redissonClient.getBucket(bucketName).get()))){
                int release = counter.getAndSet(0);
                if (release > 0){
                    redissonClient.getSemaphore(monitorSemaphore).release(release);
                }
                Thread.sleep(1000);
            }
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                Thread.sleep(1000);
                if (monitorQuery != null){
                    monitorQuery.stop();
                }
                streamingQuery.stop();
            } catch (Exception e){
                throw new RuntimeException(e);
            }
            shutdown();
        }
    }

    private StreamingQuery preprocess(Dataset<Row> data){
        try {
            StreamingQuery streamingQuery = data.writeStream()
                    .outputMode("append")
                    .foreachBatch((batchDf, batchId) -> {
                        counter.getAndAdd((int) batchDf.count());
                    }).start();
            return streamingQuery;
        } catch (Exception e){
            throw new RuntimeException(e);
        }

    }


}
