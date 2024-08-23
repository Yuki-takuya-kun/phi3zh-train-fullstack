package phi3zh.dataconverter.deduplicator;

import phi3zh.dataconverter.Converter;

public abstract class Deduplicator<T> implements Converter {
    double threshold;
    protected abstract T deduplicate(T input) throws Exception;
    public void setThreshold(double threshold){
        this.threshold = threshold;
    }
}
