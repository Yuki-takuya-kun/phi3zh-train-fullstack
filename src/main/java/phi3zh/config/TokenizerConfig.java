package phi3zh.config;

public class TokenizerConfig {
    private int size = 10;
    private String dataPath = "E:/Datasets/phi3-zh/test/cleaned_corpus";
    private String outputFile = "E:/Datasets/phi3-zh/test/tokens.jsonl";
    private String sparkAppName = "tokenizer";
    private String sparkMaster;
    private String hostName;
    private int port;

    public TokenizerConfig(
            String hostName, int port, CommonConfig commonConfig){
        this.hostName = hostName;
        this.port = port;
        this.sparkMaster = commonConfig.getSparkMaster();
    }

    public int size(){return this.size;}
    public int port(){return this.port;}
    public String getHostName(){return this.hostName;}
    public String getDataPath(){return this.dataPath;}
    public String getOutputFile(){return outputFile;}
    public String getSparkMaster(){return sparkMaster;}
    public String getSparkAppName(){return sparkAppName;}
}
