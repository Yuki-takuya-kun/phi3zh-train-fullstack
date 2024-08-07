package phi3zh.datacleaner;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import phi3zh.config.CleanerConfig;

@SpringBootTest(classes = CleanerConfig.class)
@ActiveProfiles("test")
public class TestLSHDeduplicator {

    private final LSHDeduplicator lshDeduplicator;

    @Autowired
    public TestLSHDeduplicator(LSHDeduplicator lshDeduplicator){
        this.lshDeduplicator = lshDeduplicator;
    }

    @Test
    public void testLshDeduplicate(){
        this.lshDeduplicator.clean();
    }
}
