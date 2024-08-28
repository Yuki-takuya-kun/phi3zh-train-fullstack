package phi3zh.dataconverter;

import java.io.Serializable;

// the interface that convert a data to another data
public abstract class Converter<T> implements Serializable, Runnable {

    protected abstract T load();
    protected abstract T process(T data);
    protected abstract void save(T data);
}
