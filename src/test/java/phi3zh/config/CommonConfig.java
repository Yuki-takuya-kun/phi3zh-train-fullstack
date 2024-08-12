package phi3zh.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import phi3zh.service.BackoffService;

@Configuration
@ComponentScan(basePackages = "phi3zh.common")
public class CommonConfig {

    protected int backoffMaxRetry = 3;
    protected String bootStrapServers = "172.20.45.250:9092";

    @Bean
    public String getBootStrapServers(){
        return this.bootStrapServers;
    }

    @Bean
    public int getBackoffMaxRetry(){
        return this.backoffMaxRetry;
    }

    @Bean
    public BackoffService backoffService(){
        return new BackoffService(backoffMaxRetry);
    }
}
