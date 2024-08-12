package phi3zh.common.aspects.backoff;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import phi3zh.common.utils.Backoff;
import phi3zh.config.CommonConfig;

/**
 * the implementation of exponential backoff
 */
@Aspect
@Component
public class NetExpBackoffAspect {

    private final CommonConfig commonConfig;

    @Autowired
    public NetExpBackoffAspect(CommonConfig commonConfig){
        this.commonConfig = commonConfig;
    }

    @Around("@annotation(phi3zh.common.annotations.backoff.NetExpBackoff)")
    public Object aroundExpBackoff(ProceedingJoinPoint joinPoint) throws Throwable{
        int maxRetry = commonConfig.getBackoffMaxRetry();
        String[] exceptionPrefixs = new String[]{"java.net", "javax.net"};
        Object result = Backoff.expBackoff(joinPoint, maxRetry, exceptionPrefixs);
        return result;
    }
}
