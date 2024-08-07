package phi3zh;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import phi3zh.datacleaner.LSHDeduplicator;

@SpringBootApplication
public class Phi3zhApplication {

//    @Autowired
//    private WikiCleaner wikiCleaner;
//
//    @Autowired
//    private LSHDeduplicator lshDeduplicator;

    public static void main(String[] args){
        SpringApplication springApplication = new SpringApplication(Phi3zhApplication.class);
        springApplication.setAdditionalProfiles("production");
        springApplication.run(args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext context){
        Map<String, Boolean> map = new HashMap<>();
        map.put("cleaner", false);
        map.put("deduplicator", true);
        System.out.println("step1");
        return args -> {
//            if (map.get("cleaner")){
//                WikiCleaner wikiCleaner = context.getBean(WikiCleaner.class);
//                wikiCleaner.clean();
//            }
            if (map.get("deduplicator")){
                LSHDeduplicator lshDeduplicator = context.getBean(LSHDeduplicator.class);
                lshDeduplicator.clean();
            }
        };
    }
}
