package common.annotations.networkannotations;


import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.UnknownHostException;

/**
 * ExpBackoff: the annotation that applying exponential backoff algorithm
 * maxRetry is the max time of retry while it fails
 * loggerName is the log4j logger name of the logger
 * expIgnores is the Exception list that contains the exception of the network exception such as SSLHandShakeException
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ExpBackoff{
    int maxRetry() default 5;
    String loggerName() default "NetworkErrorLogger";
    Class<? extends Throwable>[] expIgnores() default {SSLHandshakeException.class, SSLPeerUnverifiedException.class,
            UnknownHostException.class};
}


