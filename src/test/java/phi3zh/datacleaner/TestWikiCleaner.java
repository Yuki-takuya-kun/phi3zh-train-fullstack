package phi3zh.datacleaner;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import phi3zh.config.CleanerConfig;

@SpringBootTest(classes = CleanerConfig.class)
@ActiveProfiles("test")
public class TestWikiCleaner {

    private final WikiCleaner wikiCleaner;

    @Autowired
    public TestWikiCleaner(WikiCleaner wikiCleaner){
        this.wikiCleaner = wikiCleaner;
    }

    @Test
    public void testSinglePage(){
        this.wikiCleaner.clean();
    }
}
