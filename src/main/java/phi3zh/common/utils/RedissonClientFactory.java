package phi3zh.common.utils;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonClientFactory {
    private static volatile RedissonClient instance;

    private RedissonClientFactory(){}

    public static RedissonClient getInstance(Config config){
        if (instance == null){
            synchronized (RedissonClientFactory.class){
                if (instance == null){
                    instance = Redisson.create(config);
                    registerShutDownHook();
                }
            }
        }
        return instance;
    }

    public static RedissonClient getInstance(){
        return instance;
    }

    private static void registerShutDownHook(){
        Runtime.getRuntime().addShutdownHook(new Thread(RedissonClientFactory::shutdown));
    }

    public synchronized static void shutdown(){
        if (instance != null && !instance.isShutdown()){
            instance.shutdown();
            instance = null;
        }
    }
}
