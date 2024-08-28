package phi3zh.dataconverter;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public abstract class StreamConverter<T> extends Converter<T>{

    private Config redisConfig;
    private String bucketName;

    protected StreamConverter(Config redisConfig,
            String bucketName){
        this.redisConfig = redisConfig;
        this.bucketName = bucketName;
    }

    @Override
    public void run(){
        RedissonClient redissonClient = Redisson.create(redisConfig);
        while(!(Boolean)redissonClient.getBucket(bucketName).get()){
            T data0 = load();
            T data1 = process(data0);
            save(data1);
        }
        redissonClient.shutdown();
        shutdown();
    }

    protected abstract void shutdown();
}
