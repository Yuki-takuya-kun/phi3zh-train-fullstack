package phi3zh.config;

public class CommonConfig {
    private String sparkMaster = "local";
    private int backoffMaxRetry = 5;
    private String kafkaServer = "172.20.45.250:9092";
    private String redisServer = "redis://127.0.0.1:6379";
    private String checkpointLocation = "E:/Datasets/phi3-zh/test/checkpoint";

    public CommonConfig(){}

    public String getSparkMaster(){return sparkMaster;}

    public String getRedisServer(){
        return redisServer;
    }

    public String getKafkaServer(){
        return kafkaServer;
    }

    public String getCheckpointLocation(){return checkpointLocation;}

    public int getBackoffMaxRetry(){
        return backoffMaxRetry;
    }
}
