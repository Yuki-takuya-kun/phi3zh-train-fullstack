package phi3zh.datacleaner.blockers;
/**
 * FileName: ChapterBlocker.java
 * Description: block the text according to chapter
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChapterBlocker implements BlockInterface{

    @Override
    public List<String> block(String text){
        Pattern chapterPat = Pattern.compile("##+");
        Matcher chapterMat = chapterPat.matcher(text);
        List<Integer> idxList = new ArrayList<>();
        List<String> result = new ArrayList<>();

        while (chapterMat.find()){
            idxList.add(chapterMat.start());
        }

        if (idxList.size() == 0){
            result.add(text);
        } else {
            for (int i = 1; i < idxList.size(); i++){
                result.add(text.substring(idxList.get(i-1), idxList.get(i)));
            }
            result.add(text.substring(idxList.get(idxList.size()-1)));
        }

        return result;
    }
}
