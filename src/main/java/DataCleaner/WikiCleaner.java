/*
* WikiCleaner Class to get the dataset of wikiDataPath;
*
* Version: v0.0.1
* */

package DataCleaner;

import java.io.IOException;
import java.util.*;
import java.io.FileWriter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.github.houbb.opencc4j.util.ZhConverterUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


public class WikiCleaner extends AbstractCleaner {

    private String wikiDataPath; // the source datapath of the wikipedia
    private String outputDir; // the output file path of the output
    private static final Map<String, String> TAG_TRANSFORM_MAP = Stream.of(new String[][]{
            {"span", ""},
            {"em", ""},
            {"strong", ""},
            {"dfn", ""},
            {"code", "`"},
            {"div", ""},
            {"nowiki", ""},
            {"poem", ""},
            {"syntaxhighlight", "```"},
            {"source", "```"},
            {"math", "$"}
    }).collect(Collectors.toMap(data -> data[0], data -> data[1]));

    public WikiCleaner(String wikiDataPath,
                       String outputDir){
        this.wikiDataPath = wikiDataPath;
        this.outputDir = outputDir;
        this.sparkSession = SparkSession.builder()
                .appName("wikiCleaner")
                .config("spark.master", "local")
                .getOrCreate();
        this.data = this.sparkSession.read()
                .format("xml")
                .option("rowTag", "page")
                .load(this.wikiDataPath);
        this.data = this.data.select("id", "title", "revision.text");
    }

    @Override
    protected void ProcessRow(Row row){
        String title = row.getString(1);
        title = ZhConverterUtil.toSimple(title);
        if (!title.contains(":")){

            Row tmp = row.getAs(2);
            if (tmp.get(0) != null) {
                System.out.println("==========================");
                System.out.println(String.format("%s/%s.txt", this.outputDir, title));
                String page = tmp.get(0).toString();
                try (FileWriter fileWriter = new FileWriter(String.format("%s/ori/%s.txt", this.outputDir, title), true)){
                    fileWriter.write(page);
                } catch (IOException e){
                    e.printStackTrace();
                }
                String new_page = CleanElement(page);

                System.out.println(String.format("save %s page", title));
                try (FileWriter fileWriter = new FileWriter(String.format("%s/tgt/%s.txt", this.outputDir, title), true)){
                    fileWriter.write(new_page);
                } catch (IOException e){
                    e.printStackTrace();
                }
            }
        }


    }

    /**
     * Clean the page of the wikipedia
     *
     * @param page the page that needed to clean
     * */
    protected String CleanElement(String page){
        String res = "";

        page = MetaDataProcess(page);
        ArrayList<Pair<String, String>> chapters = SplitWithChapters(page);
        Stack<Pair<String, Integer>> blockStack = new Stack<>();

        for (int i = 0; i < sectionBlocks.size(); i++){
            Pair<String, Integer> cleanBlockLevel = CleanBlock(sectionBlocks.get(i));
            if (cleanBlockLevel.getLeft().length() == 0 && blockStack.size() > 0
                    && cleanBlockLevel.getRight() == blockStack.peek().getRight()){
                continue;
            } else{
                blockStack.push(cleanBlockLevel);
            }
        }

        Pair<String, Integer>[] blocks = new Pair[blockStack.size()];
        for (int i = 0; i < blockStack.size(); i++){
            blocks[i] = blockStack.get(i);
        }
        for (Pair<String, Integer> block: blocks){
            res = res + block.getLeft() + "\n";
        }
        return res;
    }

    public String test(String page){ return CleanElement(page);}


    /**
     * Split the page text into many blocks with chapter, according the first level heading
     * @param page the page to split
     * @return the List of Pair contains the each chapter title and its content
     */
    private ArrayList<Pair<String, String>> SplitWithChapters(String page){
        Pattern firstHeadingPattern = Pattern.compile("==[^=]*==");
        Matcher firstHeadingMatcher = firstHeadingPattern.matcher(page);
        ArrayList<Pair<String, String>> chapters = new ArrayList<>();
        String title = ""; // the title of each chapter
        String content = ""; // the content of each chapter
        int lastEnd = 0;

        while (firstHeadingMatcher.find()){
            content = page.substring(lastEnd, firstHeadingMatcher.start());
            chapters.add(Pair.of(title, content));
            title = page.substring(firstHeadingMatcher.start(), firstHeadingMatcher.end()).trim();
            lastEnd = firstHeadingMatcher.end();
        }
        chapters.add(Pair.of(title, page.substring(firstHeadingMatcher.end())));

        return chapters;
    }

    /**
     * get the meta information of the page, such as transform group
     * @param page the first chapter that contains the meta information
     */
    private String MetaDataProcess(String page){
        // get transform group
        Pattern transformGroupPattern = Pattern.compile("\\{\\{NoteTA.*?\\}\\}");
        Matcher transformGroupMatcher = transformGroupPattern.matcher(page);
        // the pattern that start with the sequence, such as 2=
        Pattern transformListPrefix = Pattern.compile("^\\d+=");
        Pattern publicTransformPrefix = Pattern.compile("^G\\d+=");
        Pattern desTransformPrefix = Pattern.compile("^d\\d+=");

        if (transformGroupMatcher.find()){
            Map<String, String> wordMap = new HashMap<>(); // the map that map the word to a target word
            page = page.substring(0, transformGroupMatcher.start()) + page.substring(transformGroupMatcher.end());
            String transformGroup = transformGroupMatcher.group().replaceAll("\\s+", "");
            String[] groups = transformGroup.split("|");
            // iterate every group, the first element is {{NoteTA element, ignore it
            for (int i = 1; i < groups.length; i++){
                // reset the last element, which include }}
                if (i == groups.length-1){
                    groups[i] = groups[i].replace("}}", "").replace("\n", "");
                }
                // if the group is start with description or belongs to public transform group, ignore it
                if (publicTransformPrefix.matcher(groups[i]).find() || desTransformPrefix.matcher(groups[i]).find()){
                    continue;
                }
                else{
                    // if the group is start with order, discard it
                    if (transformListPrefix.matcher(groups[i]).find()){
                        groups[i] = groups[i].substring(transformListPrefix.matcher(groups[i]).end());
                    }
                    String[] words = groups[i].split(";");
                    String tagetWord = "";
                    Map<String, String> langWordMap = new HashMap<>();
                    for (String word: words){
                        // if it is a on way transformation
                        if (word.contains("=>")){
                            String[] transWords = word.split("=>");
                            String sourceWord = transWords[0];
                            String[] langTarget = transWords[1].split(":");
                            // if the target language is zh-cn, then add it to mapper directly
                            if (langTarget[0].equals("zh-cn")){
                                page = page.replace(sourceWord, langTarget[1]);
                            }
                        } else if (word.contains(":")) {
                            String[] langWords = word.split(":"); // the first elem is lang like zh-cn
                            langWordMap.put(langWords[0], langWords[1]);
                        }
                        // ignore other situation
                    }
                    // add the transformation into wordMap
                    if (langWordMap.containsKey("zh-cn")){
                        for (Map.Entry<String, String> entry: langWordMap.entrySet()){
                            if (entry.getKey() != "zh-cn"){
                                page = page.replace(entry.getKey(), langWordMap.get("zh-cn"));
                            }
                        }
                    }

                }
            }
        }
        return page;
    }


    /**
     * Parse the xml tag and transfer or delete it, for example discard <tag> and conserve <code> tag content
     * @param text
     * @return the text that modified
     */
    private String XMLParse(String text){
        int skip = 0;
        Pattern markupPattern = Pattern.compile("<[^<]*?>");
        Pattern markupEndPattern = Pattern.compile("</[^<]*?>");
        Pattern singlePattern = Pattern.compile("<[^>]*?/>");
        //Pattern commentPattern = Pattern.compile("<!--.*-->");
        Matcher markupMatcher = markupPattern.matcher(text);
        Stack<Pair<String, Pair<Integer, Integer>>> beginsStack = new Stack<>();

        while (markupMatcher.find()){
            // skip the nested times for each time, because in every time the text will be updated
            if (skip < beginsStack.size()){
                skip ++;
                continue;
            }
            skip = 0;
            if (singlePattern.matcher(markupMatcher.group()).matches()){
                // currently could not find the single pattern that should be reserved
                text = text.substring(0, markupMatcher.start()) + text.substring(markupMatcher.end());
            }
            // if the pattern is the end markup then process the inter content
            else if (markupEndPattern.matcher(markupMatcher.group()).matches()){
                Pair<String, Pair<Integer, Integer>> beginMarkup = beginsStack.pop();
                Pair<Integer, Integer> beginMarkupSpan = beginMarkup.getRight();
                String beginMarkupContent = beginMarkup.getLeft();
                String tag = beginMarkupContent.split("\\s+")[0];
                // save and add the tag for markdown
                if (this.TAG_TRANSFORM_MAP.containsKey(tag)){
                    text = text.substring(0, beginMarkupSpan.getLeft())
                            + this.TAG_TRANSFORM_MAP.get(tag) + text.substring(beginMarkupSpan.getRight(), markupMatcher.start())
                            + text.substring(markupMatcher.end()) + this.TAG_TRANSFORM_MAP.get(tag);
                }
                // else discard the content
                else {
                    text = text.substring(0, beginMarkupSpan.getLeft()) + text.substring(markupMatcher.end());
                }
            }

            // reset the matcher
            markupMatcher = markupPattern.matcher(text);
        }
        return text;
    }


    /**
     * Parse the template tag in wikipedia with format of {{}}, many template should be conserve and other should
     * be discard
     * @param text the text that should be parse
     * @return the text after parse
     */
    private String TemplateParse(String text){
        Pattern templatePattern = Pattern.compile("\\{\\{[^\\{]*?\\}\\}");
    }


    /**
     * Transfer the blocks content into cleaned format
     * @param block the block that have been split
     * @return the pair that contain the cleaned text and the block level
     */
    private Pair<String, Integer> CleanBlock(String block){
        Stack<String> stack = new Stack<>();
        String res = "";
        int level = 0;
        String[] lines = block.split("\n");
        ArrayList<String> stringList = new ArrayList<>(Arrays.asList(lines));
        // if the first line is title, then transfer calculate its level and transfer it to markdown format
        if (IsTitle(stringList.get(0))){
            Pair<String, Integer> titleLevel = TransferAndGetLevel(stringList.get(0));
            String title = titleLevel.getLeft();
            level = titleLevel.getRight();
            res = res + title + "\n";
            stringList.remove(0);
        }
        block = String.join("\n", stringList);
        block = CleanAngleBrackets(block);
        block = CleanDoubleBracket(block);
        block = CleanDoubleBrace(block);
        //block = FilterList(block);
        block = TransferToMD(block);
        block = FilterInessential(block);
        // if the block content is zero, then return nothing
        if (block.replace("\n", "").length() > 30){
            res = ZhConverterUtil.toSimple(res + block); // transfer to simple chinese
            return Pair.of(res, level);
        } else {
            return Pair.of("", -1);
        }
    }


    // assert the string is title format or not
    private boolean IsTitle(String line){
        line = line.trim();
        if (line.startsWith("=") && line.endsWith("=")){
            return true;
        } else {return false;}
    }

    // transfer the title to the markdown format and get the title level
    private Pair<String, Integer> TransferAndGetLevel(String title){
        title = title.trim();
        int level = 0;
        for (int i = 0; i < title.length(); i++){
            if (title.charAt(i) == '='){
                level ++;
            } else {break;}
        }

        title = title.substring(level, title.length()-level).trim();

        level --;
        String prefix = "";
        for (int i = 0; i < level; i ++){
            prefix = prefix + '#';
        }
        return Pair.of(prefix + " " + title, level);
    }

    /**
     * process the angle brackets
     * @param block the block content of the section
     * @return the String object that has been cleaned
     */
    private String CleanAngleBrackets(String block){
        Stack<Pair<Integer, Integer>> pairStack = new Stack<>();
        Stack<Pair<Integer, Integer>> delIntervals = new Stack<>();
        Pattern anglePattern = Pattern.compile("<[^<]*?>");
        Pattern closePattern = Pattern.compile("</.*?>");
        Pattern singlepattern = Pattern.compile("<.*?/>");
        Pattern commentPattern = Pattern.compile("<!--.*?-->");
        Matcher angleMatcher = anglePattern.matcher(block);
        while (angleMatcher.find()){
            String matchStr = angleMatcher.group();
            // if the matchStr is the close bracket, then match the last and add it to the delIntervals
            // note that it should be merge with the last intervals.
            if (closePattern.matcher(matchStr).matches()){
                if (pairStack.size() == 1){
                    int startInterval1 = pairStack.pop().getLeft();
                    int endInterval1 = angleMatcher.end();
                    delIntervals.push(Pair.of(startInterval1, endInterval1));
                } else if (pairStack.size() == 0) {
                    delIntervals.push(Pair.of(angleMatcher.start(), angleMatcher.end()));
                }
                else {
                    pairStack.pop();
                }
            } else if (singlepattern.matcher(matchStr).matches() || commentPattern.matcher(matchStr).matches()){
                if (pairStack.size() == 0){
                    delIntervals.push(Pair.of(angleMatcher.start(), angleMatcher.end()));
                }
            }
            else {
                pairStack.push(Pair.of(angleMatcher.start(), angleMatcher.end()));
            }
        }


        //copy the stack to array and sort it according to the first element
        Pair<Integer, Integer>[] delIntervalsArray = new Pair[delIntervals.size()];
        delIntervals.toArray(delIntervalsArray);
        Arrays.sort(delIntervalsArray, Comparator.comparing(Pair<Integer, Integer>::getKey));
        String output = "";

        for (int i = 0; i < delIntervalsArray.length; i++){
            if (i == 0 && i != delIntervalsArray.length - 1){
                output = output + block.substring(0, delIntervalsArray[0].getLeft());
            } else if (i == delIntervalsArray.length - 1 && i != 0){
                output = output + block.substring(delIntervalsArray[i-1].getRight(), delIntervalsArray[i].getLeft())
                        + block.substring(delIntervalsArray[i].getRight());
            }
            else if (i == 0 && i == delIntervalsArray.length - 1){
                output = block.substring(0, delIntervalsArray[0].getLeft()) + block.substring(delIntervalsArray[0].getRight());
            }
            else {
                output = output + block.substring(delIntervalsArray[i-1].getRight(), delIntervalsArray[i].getLeft());
            }
        }
        if (delIntervalsArray.length == 0){
            return block;
        } else {
            return output;
        }

    }


    /**
     * process the double bracket of {{}}
     * @param block the block content of the section
     * @return the String object that has been cleaned
     */
    private String CleanDoubleBracket(String block){
        String res = "";
        int lastIdx = 0;
        Pattern doubleBracketPat = Pattern.compile("\\{\\{|\\}\\}");
        Matcher doubleBracketMat = doubleBracketPat.matcher(block);
        Stack<Integer> bracketStack = new Stack<>();
        Stack<Pair<String, Integer>> startStack = new Stack<>();
        while (doubleBracketMat.find()){
            if (doubleBracketMat.group().equals("{{")){
                bracketStack.push(doubleBracketMat.end());
            } else if (bracketStack.size() > 0) {

                int lastEnd = bracketStack.pop();
                String content = block.substring(lastEnd, doubleBracketMat.start());
                String[] elements = content.split("\\|");
                // if the content is the begin of the sub block
                if (elements[0].contains("begin") || elements[0].toLowerCase().trim().equals("div col")){
                    startStack.push(Pair.of("{{"+content+"}}", lastEnd-2));
                // if the content is the end of the sub block, check if it is the last block
                } else if (elements[0].contains("end") && !elements[0].equals("legend")){
                    System.out.println(content);
                    Pair<String, Integer> beginBracket = startStack.pop();
                    int startIdx = beginBracket.getRight();
                    if (startStack.size() == 0){
                        res = res + block.substring(lastIdx, startIdx);
                        lastIdx = lastEnd + 2 + content.length();
                    }

                } else if (startStack.size() == 0 && bracketStack.size() == 0){

                    // process the language block
                    if (elements[0].toLowerCase().startsWith("lang")){
                        String language = "";
                        if (elements[0].contains("-")){
                            Locale locale = new Locale(elements[0].split("-")[1]);
                            language = locale.getDisplayLanguage(Locale.CHINESE) + "：" + elements[1];
                        } else {
                            language = elements[2];
                        }
                        res = res + block.substring(lastIdx, lastEnd-2) + language;
                    }
                    // process the code block and transform it to markdown format
                    else if (elements[0].toLowerCase().trim().equals("code")){
                        res = res + block.substring(lastIdx, lastEnd-2) + "`" + elements[1] + "`";
                    }
                    // process the le or link
                    else if (elements[0].toLowerCase().equals("le") || elements[0].toLowerCase().contains("link-") ||
                    elements[0].toLowerCase().contains("-link")){
                        res = res + block.substring(lastIdx, lastEnd-2) + elements[1];
                    }
                    // process other block. remove it
                    else {
                        res = res + block.substring(lastIdx, lastEnd-2);
                    }
                    lastIdx = lastEnd + 2 + content.length();
                }
            }
        }

        res = res + block.substring(lastIdx);
        //System.out.println(res);
        return res;
    }


    /**
     * process the double braces of [[]]
     * @param block the block content of the section
     */
    private String CleanDoubleBrace(String block){
        String res = ""; // the return string
        int lastIdx = 0; // the index that is the last [[]] position
        Stack<Integer> leftStack = new Stack<>();
        Pattern bracePat = Pattern.compile("\\[\\[|\\]\\]");
        Matcher braceMat = bracePat.matcher(block);

        while(braceMat.find()){
            if (braceMat.group().equals("[[")){
                leftStack.push(braceMat.end());
            }
            // only if the left stack's size is 1, which indicate that the [[]] is the outermost
            else if (leftStack.size() == 1) {
                int lastEnd = leftStack.pop();
                String content = block.substring(lastEnd, braceMat.start());
                // if it contain the char :, such as File:
                if (content.contains(":")){
                    res = res + block.substring(lastIdx, lastEnd-2);
                } else {
                    String[] elements = content.split("\\|");
                    res = res + block.substring(lastIdx, lastEnd-2) + elements[elements.length-1];
                }
                lastIdx = braceMat.end();
            }
            // if the [[]] is in the nested, then pop it and ignore it;
            else {
                leftStack.pop();
                }
        }
        res = res + block.substring(lastIdx);
        return res;
    }


    // filter the other content that is inessential
    private String FilterInessential(String block){
        String[] lines = block.split("\n");
        String res = "";
        Pattern charPattern = Pattern.compile("[^\\\\w\\\\u4e00-\\\\u9fa5]+");
        for (String line: lines){
            // filter the :{| blocks, emply blocks and contains link blocks
            if (line.startsWith(":{|") || line.startsWith("|") || line.startsWith("{|") || line.startsWith("!") ||
                    line.trim().length() == 0 || ContainLink(line) || charPattern.matcher(line).matches()){
                continue;
            }
            // replace the indent to not indent
            else if (line.startsWith(":") || line.startsWith(";")){
                res = res + line.substring(1) + "\n";
            }
            else {
                res = res + line + "\n";
            }
        }
        return res;

    }

    // assert the text contains http links or not
    private boolean ContainLink(String text){
        Pattern linkPattern = Pattern.compile("https?://[^\\s/$.?#].[^\\s]*");
        Matcher linkMatcher = linkPattern.matcher(text);
        if (linkMatcher.find()){
            return true;
        } else {
            return false;
        }
    }


    // transfer the wikipedia format to markdown format
    private String TransferToMD(String block){
        // bold transform
        block = block.replace("'''", "__");
        // italic transform
        block = block.replace("''", "_");
        // unorder list transform
        for (int i = 5; i > 0; i --){
            String source = IntStream.range(0, i)
                    .mapToObj(j -> "*").collect(Collectors.joining());
            String target = IntStream.range(0, i-1).mapToObj(j -> " ")
                    .collect(Collectors.joining()) + "- ";
            block = block.replace(source, target);
        }
        // order list transform
        String[] orderLists = block.split("#");
        if (orderLists.length > 1){
            int order = 1;
            StringBuilder builder = new StringBuilder();
            builder.append(orderLists[0]);
            for (int i = 1; i < orderLists.length; i++){
                builder.append(order).append('.').append(orderLists[i]);
                order ++;
            }
            block = builder.toString();
        }

        return block;
    }



}
