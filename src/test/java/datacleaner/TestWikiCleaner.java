
import datacleaner.WikiCleaner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TestWikiCleaner {
    private static final Logger errorLogger = LogManager.getLogger("WikiErrorLogger");
    public static void main(String[] args){
//
//        WikiCleaner wikiCleaner = new WikiCleaner("E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml",
//                "output", 1,
//                true);
//        try {
//            wikiCleaner.clean();
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        String title = "Java";
        String path = String.format("output/source_wikitext/%s.txt", title);
        WikiCleaner wikiCleaner = new WikiCleaner(path, "test", true);
        wikiCleaner.clean();

    }
}
