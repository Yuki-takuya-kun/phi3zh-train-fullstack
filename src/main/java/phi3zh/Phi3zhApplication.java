package phi3zh;

import org.checkerframework.checker.units.qual.C;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.dataconverter.controller.WikiController;

public class Phi3zhApplication {
    public static void main(String[] args){
        WikiController controller = new WikiController();
        controller.run();
    }
}