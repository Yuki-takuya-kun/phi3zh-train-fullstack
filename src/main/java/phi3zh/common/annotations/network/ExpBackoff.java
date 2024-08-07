package phi3zh.common.annotations.network;


import org.apache.http.client.ClientProtocolException;

import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import java.lang.annotation.*;
import java.net.UnknownHostException;

/**
 * ExpBackoff: the annotation that applying exponential backoff algorithm
 * maxRetry is the max time of retry while it fails
 * loggerName is the log4j logger name of the logger
 * expIgnores is the Exception list that contains the exception of the network exception such as SSLHandShakeException
 */

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ExpBackoff{
    int maxRetry() default 5;
    Class<? extends Throwable>[] expIgnores() default {SSLHandshakeException.class, SSLPeerUnverifiedException.class,
            UnknownHostException.class};
}


