package phi3zh.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import phi3zh.common.annotations.backoff.NetExpBackoff;
import phi3zh.config.CommonConfig;

public class BackoffService {

    private int maxRetry;
    private int javaNetThrowCount = 1;

    @Autowired
    public BackoffService(int maxRetry){
        this.maxRetry = maxRetry;
    }

    @NetExpBackoff
    private void testJavaNetBackoffUnder() throws Exception{
        if (javaNetThrowCount < maxRetry){
            this.javaNetThrowCount ++;
            throw new java.net.UnknownHostException();
        }
        javaNetThrowCount = 1;
    }

    @NetExpBackoff
    public void testJavaxNetBackoffExceed() throws Exception{
        throw new java.net.UnknownHostException();
    }

    @NetExpBackoff
    public void clean() throws Exception{
        testJavaxNetBackoffExceed();
    }
}
