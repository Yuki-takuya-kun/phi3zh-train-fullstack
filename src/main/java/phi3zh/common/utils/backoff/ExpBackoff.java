package phi3zh.common.utils.backoff;

public class ExpBackoff extends Backoff {
    public ExpBackoff(String packagePrefix){
        super(packagePrefix);
    }

    public ExpBackoff(Class<? extends Throwable>[] exceptionIgnores){
        super(exceptionIgnores);
    }

    public ExpBackoff(String[] packagePrefixs){
        super(packagePrefixs);
    }

    @Override
    protected long computeWaitTime() {
        return 1*(long)Math.pow(2, attempt);
    }
}
