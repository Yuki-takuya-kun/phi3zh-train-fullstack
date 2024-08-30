package phi3zh.config;

public class WikitextCleanerConfig {
    private String sparkMaster;
    private String sparkAppName = "WikitextCleaner";
    private String wikidataPath;
    private String outputDir;
    private String targetTopicName;
    private String kafkaServer;
    private String resoucreSemaphoreName;
    private String redisServer;
    private boolean useCache;
    private boolean enableHighQualDetection;

    public WikitextCleanerConfig(String wikidataPath,
                           String outputDir,
                           String targetTopicName,
                           String resoucreSemaphoreName,
                           boolean useCache,
                           boolean enableHighQualDetection,
                           CommonConfig commonConfig){
        this.sparkMaster = commonConfig.getSparkMaster();
        this.wikidataPath = wikidataPath;
        this.outputDir = outputDir;
        this.targetTopicName = targetTopicName;
        this.resoucreSemaphoreName = resoucreSemaphoreName;
        this.useCache = useCache;
        this.enableHighQualDetection = enableHighQualDetection;
        this.kafkaServer = commonConfig.getKafkaServer();
        this.redisServer = commonConfig.getRedisServer();
    }

    public String getSparkMaster(){return sparkMaster;}

    public String getSparkAppName(){return sparkAppName;}

    public WikitextCleanerConfig setWikidataPath(String wikidataPath){
        this.wikidataPath =wikidataPath;
        return this;
    }

    public String getWikidataPathwikidataPath(){return this.wikidataPath;}

    public WikitextCleanerConfig setOutputDir(String outputDir){
        this.outputDir = outputDir;
        return this;
    }

    public String getOutputDir(){return outputDir;}

    public WikitextCleanerConfig setTargetTopicName(String targetTopicName){
        this.targetTopicName = targetTopicName;
        return this;
    }

    public String getTargetTopicName(){return this.targetTopicName;}

    public WikitextCleanerConfig setUseCache(boolean useCache){
        this.useCache = useCache;
        return this;
    }

    public boolean getUseCache(){return this.useCache;}

    public WikitextCleanerConfig setEnableHighQualDetection(boolean enableHighQualDetection){
        this.enableHighQualDetection = enableHighQualDetection;
        return this;
    }

    public boolean getEnableHighQualDetection(){return this.enableHighQualDetection;}

    public WikitextCleanerConfig setKafkaServer(String kafkaServer){
        this.kafkaServer = kafkaServer;
        return this;
    }

    public String getKafkaServer(){return this.kafkaServer;}

    public String redisServers(){return this.redisServer;}

    public WikitextCleanerConfig setResourceSemaphoreName(String resourceSemaphoreName){
        this.resoucreSemaphoreName = resourceSemaphoreName;
        return this;
    }

    public String getResoucreSemaphoreName(){return this.resoucreSemaphoreName;}

}
