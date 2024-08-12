package phi3zh.datacleaner;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.test.context.ActiveProfiles;
import phi3zh.config.CleanerConfig;
import phi3zh.config.CommonConfig;
import phi3zh.service.DataPreprocessor;

@SpringBootTest(classes = {CleanerConfig.class, CommonConfig.class})
@ActiveProfiles("test")
public class TestWikiCleaner {

    private boolean isPreprocess = false;
    private CleanerConfig cleanerConfig;
    private final WikiCleaner wikiCleaner;

    @Autowired
    public TestWikiCleaner(CleanerConfig cleanerConfig, @Lazy WikiCleaner wikiCleaner){
        this.cleanerConfig = cleanerConfig;
        this.wikiCleaner = wikiCleaner;
    }

    @BeforeEach
    public void setUp(){
        if (!this.isPreprocess){
            try {
                DataPreprocessor.wikiPreprocess(this.cleanerConfig.getWikiSourceFolder(), this.cleanerConfig.getWikiDataPath());
                this.isPreprocess = true;
            } catch (Exception e){
                e.printStackTrace();
                System.exit(1);
            }
        }
    }

    @Test
    public void testSinglePage(){
        this.wikiCleaner.clean();
    }
}
