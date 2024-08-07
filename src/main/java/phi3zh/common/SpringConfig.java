package phi3zh.common;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import phi3zh.datacleaner.LSHDeduplicator;
import phi3zh.datacleaner.WikiCleaner;

@Configuration
public class SpringConfig {

//    @Bean
//    @Profile("production")
//    public WikiCleaner wikiCleanerPro(){
//        String wikiDataPath = "E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml";
//        String outputPath = "output";
//        int maxDeqSize = 1;
//        boolean useCache = true;
//        return new WikiCleaner(wikiDataPath, outputPath, maxDeqSize, useCache);
//    }

    @Bean
    @Profile("production")
    public LSHDeduplicator lshDeduplicator(){
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
