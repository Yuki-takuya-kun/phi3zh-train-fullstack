package phi3zh.common.kafka.aspect;

import javafx.application.Application;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Import;
import phi3zh.common.annotations.backoff.NetExpBackoff;
import phi3zh.common.aspects.backoff.NetExpBackoffAspect;
import phi3zh.config.CleanerConfig;
import phi3zh.config.CommonConfig;
import phi3zh.service.BackoffService;

import javax.annotation.PostConstruct;
import java.util.Arrays;

@SpringBootTest(classes = CommonConfig.class)
public class TestNetworkBackoff {

    BackoffService backoffService;

    @Autowired
    public TestNetworkBackoff(BackoffService backoffService){
        this.backoffService = backoffService;
    }

    @Test
    public void testNetworkBackoff(){

        try {
            backoffService.clean();
        } catch (Exception e){
            e.printStackTrace();
        }

//        try {
//            backoffService.testJavaNetBackoffUnder();
//        } catch (Exception e){
//            Assertions.fail("the net bakcoff under fail");
//        }
//
//        Assertions.assertThrows(java.net.UnknownHostException.class, ()->backoffService.testJavaxNetBackoffExceed());

    }

}