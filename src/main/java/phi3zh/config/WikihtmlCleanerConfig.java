package phi3zh.config;

import java.util.List;

public class WikihtmlCleanerConfig {

    private String outputDir;

    private String kafkaServer;
    private List<String> redisServers;
    private String resouceSemaphoreName;
    private String endBucketName;
    private String sourceTopic;
    private String groupId = "topic_htmlCleaner";
    int pollNum = 10;

    public WikihtmlCleanerConfig(String outputDir,
                                 String sourceTopic,
                                 String resouceSemaphoreName,
                                 String endBucketName,
                                 CommonConfig commonConfig){
        this.outputDir = outputDir;
        this.sourceTopic = sourceTopic;
        this.resouceSemaphoreName = resouceSemaphoreName;
        this.endBucketName = endBucketName;
        this.kafkaServer = commonConfig.kafkaServer();
        this.redisServers = commonConfig.redisServers();
    }

    public WikihtmlCleanerConfig setOutputDir(String outputDir){
        this.outputDir = outputDir;
        return this;
    }

    public String outputDir(){return this.outputDir;}

    public WikihtmlCleanerConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String kafkaServer(){return this.kafkaServer;}

    public WikihtmlCleanerConfig setRedisServers(List<String> redisServers){
        this.redisServers = redisServers;
        return this;
    }

    public List<String> redisServers(){return this.redisServers;}

    public WikihtmlCleanerConfig setSourceTopic(String sourceTopic){
        this.sourceTopic = sourceTopic;
        return this;
    }

    public String sourceTopic(){return this.sourceTopic;}

    public WikihtmlCleanerConfig setGroupId(String groupId){
        this.groupId = groupId;
        return this;
    }

    public String groupId(){return this.groupId;}

    public WikihtmlCleanerConfig setPollNum(int pollNum){
        this.pollNum = pollNum;
        return this;
    }

    public int pollNum(){return this.pollNum;}

    public WikihtmlCleanerConfig setEndBucketName(String endBucketName){
        this.endBucketName = endBucketName;
        return this;
    }

    public String endBucketName(){return this.endBucketName;}

    public WikihtmlCleanerConfig setResourceSemaphore(String resourceSemaphoreName){
        this.resouceSemaphoreName = resourceSemaphoreName;
        return this;
    }

    public String resouceSemaphoreName(){return this.resouceSemaphoreName;}
}

