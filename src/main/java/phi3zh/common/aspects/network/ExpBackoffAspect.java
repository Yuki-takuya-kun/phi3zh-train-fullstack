package phi3zh.common.aspects.network;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;
import phi3zh.common.annotations.network.ExpBackoff;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * the implementation of exponential backoff
 */
@Aspect
@Component
public class ExpBackoffAspect {

//    @Pointcut("@annotation(phi3zh.common.annotations.network.ExpBackoff)")
//    public void expBackoff(){}

    @Around("@annotation(phi3zh.common.annotations.network.ExpBackoff)")
    public Object aroundExpBackoff(ProceedingJoinPoint joinPoint) throws Throwable{
        System.out.println("running inner");
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        ExpBackoff expBackoffObj = method.getAnnotation(ExpBackoff.class);
        int maxRetryTime = expBackoffObj.maxRetry();
        Class<? extends Throwable>[] exceptionIgnores;
        exceptionIgnores = expBackoffObj.expIgnores();
        int attempt = 0;
        Object result = null;
        while (attempt < maxRetryTime){
            try {
                System.out.println("attempting");
                TimeUnit.SECONDS.sleep(1*(long)Math.pow(2, attempt));
                result = joinPoint.proceed();
            } catch (Exception e){
                boolean ignore = Arrays.stream(exceptionIgnores).anyMatch(except -> except.isInstance(e));
                if (!ignore){
                    throw  e;
                }
                attempt ++;
            }
        }
        return result;
    }

}
