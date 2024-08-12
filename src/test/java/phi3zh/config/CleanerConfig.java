package phi3zh.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;
import org.apache.logging.log4j.core.config.Configurator;

@Configuration
public class CleanerConfig {

    private CommonConfig commonConfig;
    protected String wikiSourceFolder = "E://Datasets/phi3-zh/test/source_data";
    protected String wikiDataPath = "E://Datasets/phi3-zh/test/source_dataset/test.xml";

    @Autowired
    public CleanerConfig(CommonConfig commonConfig){
        this.commonConfig = commonConfig;
        Configurator.initialize(null, "src/test/resources/log4j2-test.xml");
    }

    @Bean
    @Lazy
    @Profile("test")
    public WikiCleaner wikiCleanerTest(){
        String outputPath = "E://Datasets/phi3-zh/test";
        String topicName = "wikiTest";
        int producerMaxRetry = 1;
        int consumerPollNum = 10;
        boolean useCache = false;
        boolean enableHighQualTextDetection = false;
        return new WikiCleaner(this.wikiDataPath, outputPath, topicName,
                this.commonConfig.bootStrapServers, consumerPollNum,
                commonConfig.backoffMaxRetry, useCache, enableHighQualTextDetection);
    }

    @Bean
    @Lazy
    @Profile("test")
    public LSHDeduplicator lshDeduplicatorTest(){
        String inputPath = "E://Datasets/phi3-zh/test/cleaned_corpus";
        String outputPath = "E://Datasets/phi3-zh/test/lsh_output";
        int p = 4;
        double t = 0.6;
        int k = 3;
        int b = 2;
        int r = 2;
        int seed = 1;
        return new LSHDeduplicator(inputPath, outputPath, p, t, k, b, r, seed);
    }

    public String getWikiSourceFolder(){return wikiSourceFolder;}
    public String getWikiDataPath(){return wikiDataPath;}
}
