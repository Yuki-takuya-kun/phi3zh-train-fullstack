package phi3zh.dataconverter.blocker;

public class BlockerFactory {
    public static Blocker get(String name){
        switch (name){
            case BlockerType.CHAPTER_BLOCKER:
                return ChapterBlocker.getInstance();
            case BlockerType.LENGTH_BLOCKER:
                return LengthBlocker.getInstance();
            default:
                return null;
        }
    }
}
