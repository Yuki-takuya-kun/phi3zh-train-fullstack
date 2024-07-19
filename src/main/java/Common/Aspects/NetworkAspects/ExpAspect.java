package Common.Aspects.NetworkAspects;

import Common.Annotations.NetworkAnnotations.ExpBackoff;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * the implementation of exponential backoff
 */
@Aspect
@Component
public class ExpAspect {

    @Pointcut("@annotation(ExpBackoff)")
    public void expBackoff(){}

    @Around("expBackoff()")
    public Object aroundExpBackoff(ProceedingJoinPoint joinPoint) throws Throwable{
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        ExpBackoff expBackoffObj = method.getAnnotation(ExpBackoff.class);
        int maxRetryTime = expBackoffObj.maxRetry();
        String loggerName = expBackoffObj.loggerName();
        Logger errorLogger = LogManager.getLogger(loggerName);
        Class<? extends Throwable>[] exceptionIgnores;
        exceptionIgnores = expBackoffObj.expIgnores();
        int retry = 0;
        Object result = null;
        while (retry < maxRetryTime){
            try {
                TimeUnit.SECONDS.sleep(1*(long)Math.pow(2, retry));
                result = joinPoint.proceed();
            } catch (Exception e){
                boolean ignore = false;
                for (Class<? extends Throwable> except: exceptionIgnores){
                    if (except.isInstance(e)){
                        ignore = true;
                        break;
                    }
                }
                if (ignore){
                    retry ++;
                    continue;
                } else {
                    throw e;
                }
            }
        }
        return result;
    }
}
