package phi3zh.common.annotations.backoff;


import java.lang.annotation.*;

/**
 * NetExpBackoff: the annotation that applying exponential backoff algorithm
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NetExpBackoff {
}


