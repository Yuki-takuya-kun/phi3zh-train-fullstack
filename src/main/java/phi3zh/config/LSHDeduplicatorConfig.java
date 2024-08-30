package phi3zh.config;

public class LSHDeduplicatorConfig {
    private String sparkAppName = "LSHDeduplicator";
    private String sparkMaster;
    private String dataDir;
    private String outputDir;
    private int p = 4;
    private double t = 0.6;
    private int k = 3;
    private int b = 2;
    private int r = 2;
    private int seed = 1;

    public LSHDeduplicatorConfig(String dataDir,
                                 String outputDir,
                                 CommonConfig commonConfig){
        this.dataDir = dataDir;
        this.outputDir = outputDir;
        this.sparkMaster = commonConfig.getSparkMaster();
    }

    public String getDataDir(){return this.dataDir;}

    public String getOutputDir(){return this.outputDir;}

    public String getSparkAppName(){return sparkAppName;}

    public String getSparkMaster(){return sparkMaster;}

    public int getP(){return this.p;}

    public LSHDeduplicatorConfig setP(int p){
        this.p = p;
        return this;
    }

    public int getK(){return this.k;}

    public LSHDeduplicatorConfig setK(int k){
        this.k = k;
        return this;
    }

    public int getB(){return this.b;}

    public LSHDeduplicatorConfig setB(int b){
        this.b = b;
        return this;
    }

    public int getR(){return this.r;}

    public LSHDeduplicatorConfig setR(int r){
        this.r = r;
        return this;
    }

    public int getSeed(){return this.seed;}

    public LSHDeduplicatorConfig setSeed(int seed){
        this.seed = seed;
        return this;
    }

    public double getT(){return this.t;}

    public LSHDeduplicatorConfig setT(double t){
        this.t = t;
        return this;
    }
}
