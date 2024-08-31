package phi3zh.dataconverter;

import org.checkerframework.checker.units.qual.C;
import org.junit.jupiter.api.Test;
import phi3zh.config.CommonConfig;
import phi3zh.config.TokenizerConfig;

public class TestTokenizer {
    private static String hostName = "localhost";
    private static int port = 7983;
    private Tokenizer tokenizer;

    public TestTokenizer(){
        CommonConfig commonConfig = new CommonConfig();
        TokenizerConfig tokenizerConfig = new TokenizerConfig(hostName, port, commonConfig);
        tokenizer = new Tokenizer(tokenizerConfig);
    }

    @Test
    public void testTokenizer(){
        this.tokenizer.run();
    }

}
