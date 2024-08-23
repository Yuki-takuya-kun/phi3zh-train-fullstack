package phi3zh.common.utils.backoff;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class Backoff {
    private static final int maxRetry = 5;
    protected int attempt;
    private Function<Throwable, Boolean> ignoreFunc;

    /**
     * To assert that the exception package's prefix is belongs to packagePrefix input or not
     * @param packagePrefix the prefix of the durable package
     */
    protected Backoff(String packagePrefix){
        this.ignoreFunc = (e)-> e.getClass().getPackage().getName().startsWith(packagePrefix);
    }

    /**
     * Assert the exception package's prefix is belongs to packagesPrefixs list or not
     * @param packagePrefixs the list that include prefixs
     */
    protected Backoff(String[] packagePrefixs){
        this.ignoreFunc = (e)->Arrays.stream(packagePrefixs).anyMatch(exceptPrefix->
                e.getClass().getPackage().getName().startsWith(exceptPrefix));
    }

    /**
     * Assert the exception package's prefix is belongs to exception packages list or not
     * @param exceptionIgnores
     */
    protected Backoff(Class<? extends Throwable>[] exceptionIgnores){
        this.ignoreFunc = (e)-> Arrays.stream(exceptionIgnores).anyMatch(except->except.isInstance(e));
    }

    /**
     * Every class of BackoffFactory should implements, which used to calculate the wating time if encounter exception.
     * @return the time that should wait
     */
    protected abstract long computeWaitTime();

    /**
     * wrap the function that add a backoff retry
     * @param callable the function to run
     * @return the result of the function
     * @param <T>
     * @throws Throwable
     */
    public <T> T wrap(Callable<T> callable) throws Throwable{
        this.attempt = 1;
        while (attempt < maxRetry){
            try {
                T result = callable.call();
                return result;
            } catch (Throwable e){
                boolean ignore = ignoreFunc.apply(e);
                if (!ignore){
                    throw e;
                }
                TimeUnit.SECONDS.sleep(this.computeWaitTime());
                this.attempt ++;
            }
        }
        // while attempt reach the maxRetry, then it will try last times
        return callable.call();
    }

}
