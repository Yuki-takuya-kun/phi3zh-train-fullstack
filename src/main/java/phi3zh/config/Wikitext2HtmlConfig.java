package phi3zh.config;

import org.redisson.config.Config;

public class Wikitext2HtmlConfig {
    private String sparkMaster;
    private String sourceTopic;
    private String targetTopic;
    private String kafkaServer;
    private String redisServer;
    private String resourceSemaphoreName;
    private String checkpointLocation;
    private Config redisConfig;

    private String endBucketName;
    private String sparkAppName = "Wikitext2Html";
    private String kafkaStartingOffset = "earliest";
    int pollNum = 10;

    public Wikitext2HtmlConfig(String sourceTopic,
                               String targetTopic,
                               String resouceSemaphoreName,
                               String endBucketName,
                               CommonConfig commonConfig){
        this.sourceTopic = sourceTopic;
        this.targetTopic = targetTopic;
        this.resourceSemaphoreName = resouceSemaphoreName;
        this.endBucketName = endBucketName;
        this.kafkaServer = commonConfig.getKafkaServer();
        this.redisServer = commonConfig.getRedisServer();
        this.sparkMaster = commonConfig.getSparkMaster();
        this.checkpointLocation = commonConfig.getCheckpointLocation();
        redisConfig = new Config();
        redisConfig.useSingleServer().setAddress(redisServer);
    }

    public String getSparkMaster(){return this.sparkMaster;}

    public String getSparkAppName(){return this.sparkAppName;}

    public String getKafkaStartingOffset(){return this.kafkaStartingOffset;}

    public String getCheckpointLocation(){return this.checkpointLocation;}

    public Wikitext2HtmlConfig setSourceTopic(String sourceTopic){
        this.sourceTopic = sourceTopic;
        return this;
    }

    public String getSourceTopic(){return this.sourceTopic;}

    public Wikitext2HtmlConfig setTargetTopic(String targetTopic){
        this.targetTopic = targetTopic;
        return this;
    }

    public String getTargetTopic(){return this.targetTopic;}

    public Wikitext2HtmlConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String getKafkaServer(){return this.kafkaServer;}

    public Wikitext2HtmlConfig setPollNum(int pollNum){
        this.pollNum = pollNum;
        return this;
    }

    public int getPollNum(){return this.pollNum;}

    public Wikitext2HtmlConfig setResourceSemaphoreName(String resourceSemaphoreName){
        this.resourceSemaphoreName = resourceSemaphoreName;
        return this;
    }

    public String getResourceSemaphoreName(){return this.resourceSemaphoreName;}

    public Wikitext2HtmlConfig setEndBucketName(String endBucketName){
        this.endBucketName = endBucketName;
        return this;
    }

    public String getEndBucketName(){return this.endBucketName;}

    public String getRedisServer(){return this.redisServer;}

    public Config getRedisConfig(){return this.redisConfig;}
}
