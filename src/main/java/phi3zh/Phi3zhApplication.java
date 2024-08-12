package phi3zh;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import phi3zh.datacleaner.WikiCleaner;
import phi3zh.datacleaner.LSHDeduplicator;

@SpringBootApplication
public class Phi3zhApplication {

    private WikiCleaner wikiCleaner;

    private LSHDeduplicator lshDeduplicator;

    @Autowired
    public Phi3zhApplication(@Lazy WikiCleaner wikiCleaner, @Lazy LSHDeduplicator lshDeduplicator){
        this.wikiCleaner = wikiCleaner;
        this.lshDeduplicator = lshDeduplicator;
    }

    public static void main(String[] args){

        SpringApplication springApplication = new SpringApplication(Phi3zhApplication.class);
        springApplication.setAdditionalProfiles("production");
        springApplication.run(args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext context){
        Map<String, Boolean> map = new HashMap<>();
        map.put("cleaner", true);
        map.put("deduplicator", false);
        return args -> {
            if (map.get("cleaner")){
                this.wikiCleaner.clean();
            }
            if (map.get("deduplicator")){
                this.lshDeduplicator.clean();
            }
       };
    }
}
