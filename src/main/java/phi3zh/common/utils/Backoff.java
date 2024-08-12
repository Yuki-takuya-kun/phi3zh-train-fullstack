package phi3zh.common.utils;

import org.aspectj.lang.ProceedingJoinPoint;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * the class that contains many algorithm of backoff, currently has been implement expBackoff
 * the function given will be retry in MaxRetry times
 */
public class Backoff {

    /**
     * basic algorithm of the expbackoff
     * @param callable the function that the function proceed
     * @param maxRetry the max retry time in the expbackoff
     * @param function the function that assert should retry or not while encounter exception
     * @return the result of the joinPoint proceed
     */
    private static <T> T basicExpBackoff(Callable<T> callable, int maxRetry, Function<Throwable, Boolean> function) throws Throwable{
        int attempt = 1;
        while (attempt < maxRetry){
            try {
                T result = callable.call();
                return result;
            } catch (Throwable e){
                boolean ignore = function.apply(e);
                System.out.println("should ignore:" + ignore);
                if (!ignore){
                    throw  e;
                }
                TimeUnit.SECONDS.sleep(1*(long)Math.pow(2, attempt));
                attempt ++;
            }
        }
        // while attempt reach the maxRetry, then it will try last times
        return callable.call();
    }

    private static Object basicExpBackoff(ProceedingJoinPoint joinPoint, int maxRetry, Function<Throwable, Boolean> function)
            throws Throwable{
        int attempt = 1;
        while (attempt < maxRetry){
            try {
                Object result = joinPoint.proceed();
                return result;
            } catch (Throwable e){
                boolean ignore = function.apply(e);
                if (!ignore){
                    throw  e;
                }
                TimeUnit.SECONDS.sleep(1*(long)Math.pow(2, attempt));
                attempt ++;
            }
        }
        // while attempt reach the maxRetry, then it will try last times
        return joinPoint.proceed();
    }

    // the exceptionIgnores contains the exceptions of the class
    public static <T> T expBackoff(Callable<T> callable, int maxRetry,
                                    Class<? extends Throwable>[] exceptionIgnores) throws Throwable{
        Function<Throwable, Boolean> inIgnoreList = (e)-> Arrays.stream(exceptionIgnores).anyMatch(except->except.isInstance(e));
        return Backoff.basicExpBackoff(callable, maxRetry, inIgnoreList);
    }

    public static Object expBackoff(ProceedingJoinPoint joinPoint, int maxRetry,
                                   Class<? extends Throwable>[] exceptionIgnores) throws Throwable{
        Function<Throwable, Boolean> inIgnoreList = (e)-> Arrays.stream(exceptionIgnores).anyMatch(except->except.isInstance(e));
        return Backoff.basicExpBackoff(joinPoint, maxRetry, inIgnoreList);
    }

    public static <T> T expBackoff(Callable<T> callable, int maxRetry,
                                   String packagePrefix) throws Throwable{
        Function<Throwable, Boolean> inPackage = (e)-> e.getClass().getPackage().getName().startsWith(packagePrefix);
        return Backoff.basicExpBackoff(callable, maxRetry, inPackage);
    }

    public static Object expBackoff(ProceedingJoinPoint joinPoint, int maxRetry,
                                    String packagePrefix) throws Throwable{
        Function<Throwable, Boolean> inPackage = (e)-> e.getClass().getPackage().getName().startsWith(packagePrefix);
        return Backoff.basicExpBackoff(joinPoint, maxRetry, inPackage);
    }

    public static <T> T expBackoff(Callable<T> callable, int maxRetry,
                                    String[] packagePrefixs) throws Throwable{
        Function<Throwable, Boolean> inPackages = (e)->Arrays.stream(packagePrefixs).anyMatch(exceptPrefix->
                e.getClass().getPackage().getName().startsWith(exceptPrefix));
        return Backoff.basicExpBackoff(callable, maxRetry, inPackages);
    }

    public static Object expBackoff(ProceedingJoinPoint joinPoint, int maxRetry,
                                    String[] packagePrefixs) throws Throwable{
        Function<Throwable, Boolean> inPackages = (e)->Arrays.stream(packagePrefixs).anyMatch(exceptPrefix->
                e.getClass().getPackage().getName().startsWith(exceptPrefix));
        return Backoff.basicExpBackoff(joinPoint, maxRetry, inPackages);
    }

    public static <T> T netExpBackoff(Callable<T> callable, int maxRetry) throws Throwable{
        String[] exceptionPrefixs = new String[]{"java.net", "javax.net"};
        return expBackoff(callable, maxRetry, exceptionPrefixs);
    }


}
