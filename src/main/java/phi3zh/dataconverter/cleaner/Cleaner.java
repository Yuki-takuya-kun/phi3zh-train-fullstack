package phi3zh.dataconverter.cleaner;

import phi3zh.dataconverter.Converter;

public abstract class Cleaner<T> implements Converter {
    protected abstract T clean(T input);
}
