package phi3zh.dataconverter;

import org.junit.jupiter.api.Test;
import phi3zh.config.TokenizerConfig;

public class TestTokenizer {
    private static String hostName = "localhost";
    private static int port = 7983;
    private Tokenizer tokenizer;

    public TestTokenizer(){
        TokenizerConfig tokenizerConfig = new TokenizerConfig(hostName, port);
        tokenizer = new Tokenizer(tokenizerConfig);
    }

    @Test
    public void testTokenizer(){
        this.tokenizer.run();
    }

}
