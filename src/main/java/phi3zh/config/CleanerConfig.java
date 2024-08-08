package phi3zh.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;

@Configuration
public class CleanerConfig {

    private String bootStrapServers = "172.20.45.250:9092";

    @Bean
    @Profile("production")
    @Lazy
    public WikiCleaner wikiCleanerPro(){
        String wikiDataPath = "E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml";
        String outputPath = "output";
        String topicName = "wikiCleaner";
        boolean useCache = false;
        int consumerPollNum = 10;
        return new WikiCleaner(wikiDataPath, outputPath, topicName, this.bootStrapServers, consumerPollNum, useCache);
    }

    @Bean
    @Profile("production")
    @Lazy
    public LSHDeduplicator lshDeduplicatorPro(){
        String inputPath = "D://projects/phi3-zh/output/cleaned_corpus";
        String outputPath = "output/lsh_output";
        int p = 4;
        double t = 0.6;
        int k = 3;
        int b = 2;
        int r = 2;
        int seed = 1;
        return new LSHDeduplicator(inputPath, outputPath, p, t, k, b, r, seed);
    }
}
