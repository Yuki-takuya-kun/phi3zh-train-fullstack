package phi3zh.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;

@Configuration
public class CleanerConfig {

    private String bootStrapServers = "172.20.45.250:9092";

    @Bean
    @Profile("test")
    public WikiCleaner wikiCleanerTest(){
        String wikiDataPath = "test/source_files/万维网.xml";
        String outputPath = "test";
        int consumerPollNum = 10;
        boolean useCache = false;
        return new WikiCleaner(wikiDataPath, outputPath, this.bootStrapServers, consumerPollNum, useCache);
    }

    @Bean
    @Profile("test")
    public LSHDeduplicator lshDeduplicatorTest(){
        String inputPath = "D://projects/phi3-zh/test/cleaned_corpus";
        String outputPath = "test/lsh_output";
        int p = 4;
        double t = 0.6;
        int k = 3;
        int b = 2;
        int r = 2;
        int seed = 1;
        return new LSHDeduplicator(inputPath, outputPath, p, t, k, b, r, seed);
    }
}
