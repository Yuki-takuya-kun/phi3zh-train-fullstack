package phi3zh.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;
import org.apache.logging.log4j.core.config.Configurator;

@Configuration
public class CleanerConfig {

    private String bootStrapServers = "172.20.45.250:9092";

    public CleanerConfig(){
        Configurator.initialize(null, "src/test/resources/log4j2-test.xml");
    }

    @Bean
    @Profile("test")
    public WikiCleaner wikiCleanerTest(){
        String wikiDataPath = "test/source_files/万维网.xml";
        String outputPath = "test";
        String topicName = "wikiTest";
        int consumerPollNum = 10;
        boolean useCache = false;
        boolean parallel = false;
        return new WikiCleaner(wikiDataPath, outputPath, topicName, this.bootStrapServers, consumerPollNum, useCache);
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
