package phi3zh.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import phi3zh.dataconverter.deduplicator.LSHDeduplicator;
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

    public String getWikiSourceFolder(){return wikiSourceFolder;}
    public String getWikiDataPath(){return wikiDataPath;}
}
