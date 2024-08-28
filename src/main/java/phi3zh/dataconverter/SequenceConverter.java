package phi3zh.dataconverter;

public abstract class SequenceConverter<T> extends Converter<T>{
    @Override
    public void run(){
        T data0 = load();
        T data = process(data0);
        save(data);
    }
}
