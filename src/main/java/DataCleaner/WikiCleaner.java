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

public class WikiCleaner extends AbstractCleaner {

    private String wikiDataPath;
    private String outputDir;
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
        ArrayList<String> sectionBlocks = SplitBlocks(page);
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
     * Split the page text into many blocks with section
     * @param page the page to split
     */
    private ArrayList<String> SplitBlocks(String page){
        String block = "";
        ArrayList<String> res = new ArrayList<>();
        for (String line: page.split("\n")){
            if (!IsTitle(line)){
                block = block + line + "\n";
            }
            else {
                res.add(block);
                block = line + "\n";
            }
        }
        if (block.length() > 0){
            res.add(block);
        }
        return res;
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
