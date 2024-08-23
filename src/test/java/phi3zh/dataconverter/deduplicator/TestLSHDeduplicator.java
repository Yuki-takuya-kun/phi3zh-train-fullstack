package phi3zh.dataconverter.deduplicator;

import org.junit.jupiter.api.Test;
import phi3zh.config.LSHDeduplicatorConfig;

public class TestLSHDeduplicator {

    private static final String dataDir = "E://Datasets/phi3-zh/test/cleaned_corpus";
    private static final String outputDir = "E://Datasets/phi3-zh/test/lsh_output";
    private LSHDeduplicator lshDeduplicator;

    public TestLSHDeduplicator(){
        LSHDeduplicatorConfig lshDeduplicatorConfig = new LSHDeduplicatorConfig(dataDir, outputDir);
        lshDeduplicator = new LSHDeduplicator(lshDeduplicatorConfig);
    }

    @Test
    public void testLshDeduplicate() throws Exception{
        this.lshDeduplicator.run();
    }
}
