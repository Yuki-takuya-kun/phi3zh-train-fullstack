package phi3zh.config;

import org.redisson.config.Config;

public class WikihtmlCleanerConfig {

    private String outputDir;

    private String kafkaServer;
    private String kafkaStratingOffset = "earliest";
    private String sparkAppName = "WikihtmlCleaner";
    private String sparkMaster;
    private String redisServer;
    private String resouceSemaphoreName;
    private String endBucketName;
    private String sourceTopic;
    private String checkpointLocation;
    private String groupId = "topic_htmlCleaner";
    private Config redisConfig;
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
        this.kafkaServer = commonConfig.getKafkaServer();
        this.redisServer = commonConfig.getRedisServer();
        this.sparkMaster = commonConfig.getSparkMaster();
        this.checkpointLocation = commonConfig.getCheckpointLocation();
        redisConfig = new Config();
        redisConfig.useSingleServer().setAddress(redisServer);
    }

    public String getSparkMaster(){return sparkMaster;}

    public String getSparkAppName(){return sparkAppName;}

    public String getKafkaStratingOffset(){return kafkaStratingOffset;}

    public String getCheckpointLocation(){return checkpointLocation;}

    public WikihtmlCleanerConfig setOutputDir(String outputDir){
        this.outputDir = outputDir;
        return this;
    }

    public String getOutputDir(){return this.outputDir;}

    public WikihtmlCleanerConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String getKafkaServer(){return this.kafkaServer;}

    public Config getRedisConfig(){return this.redisConfig;}

    public WikihtmlCleanerConfig setSourceTopic(String sourceTopic){
        this.sourceTopic = sourceTopic;
        return this;
    }

    public String getSourceTopic(){return this.sourceTopic;}

    public WikihtmlCleanerConfig setGroupId(String groupId){
        this.groupId = groupId;
        return this;
    }

    public String getGroupId(){return this.groupId;}

    public WikihtmlCleanerConfig setPollNum(int pollNum){
        this.pollNum = pollNum;
        return this;
    }

    public int getPollNum(){return this.pollNum;}

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

