package phi3zh.dataconverter.blocker;
import java.util.ArrayList;
import java.util.List;

public class LengthBlocker extends Blocker {
    private static LengthBlocker instance;

    private LengthBlocker(){}

    public static LengthBlocker getInstance(){
        if (instance == null){
            synchronized (LengthBlocker.class){
                if (instance == null){
                    instance = new LengthBlocker();
                }
            }
        }
        return instance;
    }

    @Override
    public List<String> block(String text){
        List<String> result = new ArrayList<>();
        return result;
    }
}
