package phi3zh.config;

import org.redisson.config.Config;

public class Wikitext2HtmlConfig {
    private String sourceTopic;
    private String targetTopic;
    private String kafkaServer;
    private String redisServer;
    private String resourceSemaphoreName;
    private Config redisConfig;

    private String endBucketName;
    private String groupId = "Wikitext2Html";
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
        this.kafkaServer = commonConfig.kafkaServer();
        this.redisServer = commonConfig.redisServer();
        redisConfig = new Config();
        redisConfig.useSingleServer().setAddress(redisServer);
    }

    public Wikitext2HtmlConfig setSourceTopic(String sourceTopic){
        this.sourceTopic = sourceTopic;
        return this;
    }

    public String sourceTopic(){return this.sourceTopic;}

    public Wikitext2HtmlConfig setTargetTopic(String targetTopic){
        this.targetTopic = targetTopic;
        return this;
    }

    public String targetTopic(){return this.targetTopic;}

    public Wikitext2HtmlConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String kafkaServer(){return this.kafkaServer;}

    public Wikitext2HtmlConfig setPollNum(int pollNum){
        this.pollNum = pollNum;
        return this;
    }

    public int pollNum(){return this.pollNum;}

    public Wikitext2HtmlConfig setResourceSemaphoreName(String resourceSemaphoreName){
        this.resourceSemaphoreName = resourceSemaphoreName;
        return this;
    }

    public String resourceSemaphoreName(){return this.resourceSemaphoreName;}

    public Wikitext2HtmlConfig setEndBucketName(String endBucketName){
        this.endBucketName = endBucketName;
        return this;
    }

    public String endBucketName(){return this.endBucketName;}

    public Wikitext2HtmlConfig setGroupId(String groupId){
        this.groupId = groupId;
        return this;
    }

    public String groupId(){return this.groupId;}

    public String redisServer(){return this.redisServer;}

    public Config redisConfig(){return this.redisConfig;}
}
