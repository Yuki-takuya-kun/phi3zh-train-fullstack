package phi3zh.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.*;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;

@Configuration
public class CleanerConfig {

    private CommonConfig commonConfig;

    @Autowired
    public CleanerConfig(CommonConfig commonConfig){
        this.commonConfig = commonConfig;
    }

    @Bean
    @Profile("production")
    @Lazy
    public WikiCleaner wikiCleanerPro(){
        String wikiDataPath = "E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml";
        //String wikiDataPath ="E://Datasets/phi3-zh/test/source_dataset/test.xml";
        String outputPath = "E://Datasets/phi3-zh/output";
        String topicName = "wikiCleaner";
        boolean useCache = false;
        boolean enableHighQualDetection = true;
        int consumerPollNum = 10;
        return new WikiCleaner(wikiDataPath, outputPath, topicName,
                this.commonConfig.getBootStrapServers(), consumerPollNum,
                commonConfig.backoffMaxRetry, useCache, enableHighQualDetection);
    }

    @Bean
    @Profile("production")
    @Lazy
    public LSHDeduplicator lshDeduplicatorPro(){
        String inputPath = "E://Datasets/phi3-zh/output/cleaned_corpus";
        String outputPath = "E://Datasets/output/lsh_output";
        int p = 4;
        double t = 0.6;
        int k = 3;
        int b = 2;
        int r = 2;
        int seed = 1;
        return new LSHDeduplicator(inputPath, outputPath, p, t, k, b, r, seed);
    }
}
