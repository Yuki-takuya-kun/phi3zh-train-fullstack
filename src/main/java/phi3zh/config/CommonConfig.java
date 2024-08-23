package phi3zh.config;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CommonConfig {
    private int backoffMaxRetry = 5;
    private String kafkaServer = "172.20.45.250:9092";
    private List<String> redisServers = Stream.of(new String[]{
            "redis://127.0.0.1:6379"
    }).collect(Collectors.toList());

    public CommonConfig(){}

    public List<String> redisServers(){
        return redisServers;
    }

    public String kafkaServer(){
        return kafkaServer;
    }

    public int backoffMaxRetry(){
        return backoffMaxRetry;
    }
}
