package DataCleaner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Collection;
import org.apache.commons.lang3.tuple.Pair;
import java.util.Collections;

public class TestWikiCleaner {
    public static void main(String[] args){
//        WikiCleaner wikiCleaner = new WikiCleaner("E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml",
//        "output", 1,
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
