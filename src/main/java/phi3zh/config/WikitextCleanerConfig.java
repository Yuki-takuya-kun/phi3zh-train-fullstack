package phi3zh.config;

import java.util.List;

public class WikitextCleanerConfig {
    private String wikidataPath;
    private String outputDir;
    private String targetTopicName;
    private String kafkaServer;
    private String resoucreSemaphoreName;
    private List<String> redisServers;
    private boolean useCache;
    private boolean enableHighQualDetection;

    public WikitextCleanerConfig(String wikidataPath,
                           String outputDir,
                           String targetTopicName,
                           String resoucreSemaphoreName,
                           boolean useCache,
                           boolean enableHighQualDetection,
                           CommonConfig commonConfig){
        this.wikidataPath = wikidataPath;
        this.outputDir = outputDir;
        this.targetTopicName = targetTopicName;
        this.resoucreSemaphoreName = resoucreSemaphoreName;
        this.useCache = useCache;
        this.enableHighQualDetection = enableHighQualDetection;
        this.kafkaServer = commonConfig.kafkaServer();
        this.redisServers = commonConfig.redisServers();
    }

    public WikitextCleanerConfig setWikidataPath(String wikidataPath){
        this.wikidataPath =wikidataPath;
        return this;
    }

    public String wikidataPath(){return this.wikidataPath;}

    public WikitextCleanerConfig setOutputDir(String outputDir){
        this.outputDir = outputDir;
        return this;
    }

    public String outputDir(){return outputDir;}

    public WikitextCleanerConfig setTargetTopicName(String targetTopicName){
        this.targetTopicName = targetTopicName;
        return this;
    }

    public String targetTopicName(){return this.targetTopicName;}

    public WikitextCleanerConfig setUseCache(boolean useCache){
        this.useCache = useCache;
        return this;
    }

    public boolean useCache(){return this.useCache;}

    public WikitextCleanerConfig setEnableHighQualDetection(boolean enableHighQualDetection){
        this.enableHighQualDetection = enableHighQualDetection;
        return this;
    }

    public boolean enableHighQualDetection(){return this.enableHighQualDetection;}

    public WikitextCleanerConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String kafkaServer(){return this.kafkaServer;}

    public WikitextCleanerConfig setRedisServers(List<String> redisServers){
        this.redisServers = redisServers;
        return this;
    }

    public List<String> redisServers(){return this.redisServers;}

    public WikitextCleanerConfig setResourceSemaphoreName(String resourceSemaphoreName){
        this.resoucreSemaphoreName = resourceSemaphoreName;
        return this;
    }

    public String resourceSemaphoreName(){return this.resoucreSemaphoreName;}

}
