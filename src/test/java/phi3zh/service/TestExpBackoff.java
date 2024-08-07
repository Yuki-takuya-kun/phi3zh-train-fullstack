package phi3zh.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import phi3zh.common.aspects.network.ExpBackoffAspect;
import phi3zh.Phi3zhApplication;

@SpringBootTest(classes = Phi3zhApplication.class)
public class TestExpBackoff {
    @Autowired
    private TestExpBackoffService testExpBackoffService;

    @Autowired
    private ExpBackoffAspect expBackoffAspect;

    @Test
    public void test(){
        try {
            testExpBackoffService.testMethod();
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
