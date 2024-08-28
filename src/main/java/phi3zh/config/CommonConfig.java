package phi3zh.config;

import java.util.List;

public class CommonConfig {
    private int backoffMaxRetry = 5;
    private String kafkaServer = "172.20.45.250:9092";
    private String redisServer = "redis://127.0.0.1:6379";

    public CommonConfig(){}

    public String redisServer(){
        return redisServer;
    }

    public String kafkaServer(){
        return kafkaServer;
    }

    public int backoffMaxRetry(){
        return backoffMaxRetry;
    }
}
