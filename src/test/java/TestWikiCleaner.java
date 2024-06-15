
import DataCleaner.WikiCleaner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestWikiCleaner {
    public static void main(String[] args){
        WikiCleaner wikiCleaner = new WikiCleaner("E:\\Datasets\\wiki\\zhwiki-20240520-pages-articles.xml\\zhwiki-20240520-pages-articles.xml",
                "output", 1000);
        wikiCleaner.clean();




    }


}
