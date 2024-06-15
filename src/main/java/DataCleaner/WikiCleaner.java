/*
* WikiCleaner Class to get the dataset of wikiDataPath;
*
* Version: v0.0.1
* */

package DataCleaner;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.*;

import java.net.URI;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.*;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.methods.HttpGet;

import com.github.houbb.opencc4j.util.ZhConverterUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.stream.IntStream;


public class WikiCleaner extends AbstractCleaner<Pair<String, String>> {

    private String wikiDataPath; // the source datapath of the wikipedia
    private String outputDir; // the output file path of the output
    private String configPath = "configuration/wiki.xml"; // the path of configuration file
    private List<String> titles = new ArrayList<>(); // the lists contains the titles that should be get

    private final int STARTSWITH = 1;
    private final int EQUALS = 2;

    private Map<String, Integer> templateDiscard = Stream.of(new Object[][]{
            {"cite", this.STARTSWITH},
            {"ref", this.STARTSWITH},
            {"Main", this.STARTSWITH},
            {"see also", this.EQUALS},
            {"Wikisource further reading", this.EQUALS},
            {"Wiktionary", this.EQUALS},
            {"Commonscat", this.EQUALS}
    }).collect(Collectors.toMap(data-> (String) data[0], data-> (Integer)data[1]));

    private Map<String, Integer> linkDiscard = Stream.of(new Object[][]{
            {"file:", this.STARTSWITH},
            {"category:", this.STARTSWITH},
    }).collect(Collectors.toMap(data->(String)data[0], data->(Integer)data[1]));

    // configuration settings
    private String language = "zh";
    private int frequency = 6;
    private int titleNumPerRequest = 10;

    public WikiCleaner(String wikiDataPath,
                       String outputDir,
                       int maxDequeSize){
        this.wikiDataPath = wikiDataPath;
        this.outputDir = outputDir;
        this.maxDequeSize = maxDequeSize;

        // load configuration files
        LoadConfiguration();
    }


    private void LoadConfiguration(){
        try {
            DocumentBuilderFactory  factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document config = builder.parse(this.configPath);

            config.getDocumentElement().normalize();
            this.language = config.getElementsByTagName("language").item(0).getTextContent().trim();
            this.frequency = Integer.parseInt(config.getElementsByTagName("frequency").item(0).getTextContent().trim());
            this.titleNumPerRequest = Integer.parseInt(config.getElementsByTagName("title-num-per-request").item(0)
                    .getTextContent().trim());
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    public void test(){
        System.out.println(this.titles);
    }

    @Override
    protected void saveElement(Pair<String, String> element) {

    }

    @Override
    protected void getElements(){
        System.out.println(String.format("collect pages from %s", this.wikiDataPath));
        // load spark only if no title file input
        SparkSession sparkSession = SparkSession.builder()
                .appName("wikiCleaner")
                .config("spark.master", "local")
                .getOrCreate();
        Dataset<Row> data = sparkSession.read()
                .format("xml")
                .option("rowTag", "page")
                .load(this.wikiDataPath);
        data = data.select("title", "revision.text");
        System.out.println("finish collections");
        data.foreach(row -> {
            System.out.println(row);
            // filter the page that not a wiki and redirected
//            String title = row.getString(0);
//            System.out.println(title);
//            Row tmp = row.getAs(1);
//            String content = tmp.getString(0);
//            System.out.println(content);
//            // remove the wikipedia: help: pages and redirect pages
//            if (!title.contains(":") && content.contains("#REDIRECT")){
//                this.data.put(Pair.of(title, cleanWikitext(content)));
//            }
        });
        System.out.println("end process");
    }

    @Override
    protected Pair<String, String> cleanElement(Pair<String, String> element){

//        String apiURL = String.format("https://%s.wikipedia.org/w/api.php", this.language);
//
//        try (CloseableHttpClient client = HttpClients.createDefault()) {
//            int count = 1;
//            List<String> titleList = new ArrayList<>();
//            // get the page content while reach number per request
//            for (int i = 0; i < this.titles.size(); i++){
//                titleList.add(this.titles.get(i));
//                if ( count % this.frequency != 0 && i != this.titles.size()-1){
//                    count ++;
//                } else {
//                    count = 1;
//                    String titles = String.join("|", titleList);
//                    URI uri = new URIBuilder(apiURL)
//                            .setParameter("action", "query")
//                            .setParameter("prop", "extracts")
//                            .setParameter("format", "json")
//                            .setParameter("titles", titles).build();
//                    System.out.println(uri);
//                    HttpGet httpGet = new HttpGet(uri);
//                    HttpResponse response = client.execute(httpGet);
//                    JsonObject responseData = JsonParser.parseString(EntityUtils.toString(response.getEntity()))
//                            .getAsJsonObject().getAsJsonObject("query").getAsJsonObject("pages");
//                    System.out.println(responseData);
//
//                    for (String id: responseData.keySet()){
//                        // if the title not in wikipedia, output the warning information
//                        if (Integer.parseInt(id) < 0){
//                            System.out.println(String.format("title %s is not assess in wikipedia",
//                                    responseData.getAsJsonObject(id).getAsJsonObject("title").getAsString()));
//                        }
//                        else {
//                            String title = responseData.getAsJsonObject(id).getAsJsonObject("title").getAsString();
//                            String content = responseData.getAsJsonObject(id)
//                                    .getAsJsonObject("extract").getAsString();
//                            this.data.offer(Pair.of(title, content));
//                        }
//                    }
//                    // wait times to get next request
//                    Thread.sleep(this.frequency * 1000);
//                }
//            }
//
//        } catch (Exception e){
//            e.printStackTrace();
//        }
        return Pair.of("", "");
    }


    /**
     * clean the wikitext
     * @param page the wikitext origin files
     * @return
     */
    private String cleanWikitext(String page){
        List<String> removeRegs = new ArrayList<>();
        removeRegs.add("<!--.*?-->"); // comments
        removeRegs.add("<ref.*?/>|<ref.*?>.*?</ref>"); // ref links
        page = page.replaceAll(String.join("|", removeRegs), "");
        Pattern bracketPattern = Pattern.compile("\\[\\[|\\]\\]|\\{\\{|\\}\\}");
        Matcher bracketMatcher = bracketPattern.matcher(page);

        Deque<Pair<String, Integer>> deque = new ArrayDeque<>();
        Stack<Pair<String, Integer>> refBlockBegin = new Stack<>();
        List<Pair<Integer, Integer>> delIntervals = new ArrayList<>();
        while (bracketMatcher.find()){
            if (bracketMatcher.group().equals("{{") || bracketMatcher.group().equals("[[")){
                deque.addLast(Pair.of(bracketMatcher.group(), bracketMatcher.start()));
            } else {
                Pair<String, Integer> left = deque.pollLast();
                Pair<String, Integer> right = Pair.of(bracketMatcher.group(), bracketMatcher.end());
                String content = page.substring(left.getRight(), right.getRight()).toLowerCase();
                if (left.getLeft().equals("\\[\\[")){
                    content = content.substring(2, content.length()-2).toLowerCase();
                    if (shouleDeleteLink(content)){
                        delIntervals.add(Pair.of(left.getRight(), right.getRight()));
                    }
                }
                else if (left.getLeft().equals("{{")){
                    String templateName = content
                            .substring(2, content.length()-2)
                            .toLowerCase().split("|")[0].trim();
                    if (shouldDeleteTemplate(templateName)){
                        delIntervals.add(Pair.of(left.getRight(), right.getRight()));
                    }
                    // remove ref blocks
                    else if (templateName.equals("refbegin")){
                        refBlockBegin.push(Pair.of(templateName, left.getRight()));
                    } else if (templateName.equals("refend")){
                        Pair<String, Integer> blockBegin = refBlockBegin.pop();
                        delIntervals.add(Pair.of(blockBegin.getRight(), right.getRight()));
                    }
                }
            }
        }

        // merge delIntervals
        List<Pair<Integer, Integer>> newDelIntervals = new ArrayList<>();
        delIntervals.sort(Comparator.comparingInt(Pair::getLeft));
        int size = delIntervals.size();
        for (int i =0; i < size; i++){
            if (i == 0){
                newDelIntervals.add(delIntervals.remove(0));
            } else {
                Pair<Integer, Integer> first = newDelIntervals.remove(newDelIntervals.size()-1);
                Pair<Integer, Integer> second = delIntervals.remove(0);
                if (first.getRight() < second.getLeft()){
                    newDelIntervals.add(first);
                    newDelIntervals.add(second);
                }
                else {
                    newDelIntervals.add(first);
                }
            }
        }

        int lastEnd = 0;
        String res = "";
        for (Pair<Integer, Integer> interval: newDelIntervals){
            res = res + page.substring(lastEnd, interval.getLeft());
            lastEnd = interval.getRight();
        }
        res = res + page.substring(lastEnd);
        return res;

    }


    private boolean shouleDeleteLink(String link){
        for (String key: this.linkDiscard.keySet()){
            if (this.linkDiscard.get(key).equals(this.STARTSWITH) && link.startsWith(key.toLowerCase())){
                return true;
            }
            else if (this.linkDiscard.get(key).equals(this.EQUALS) && link.equals(key.toLowerCase())){
                return true;
            }
        }
        return false;
    }


    private boolean shouldDeleteTemplate(String templateName){
        for (String key: this.templateDiscard.keySet()){
            if (this.templateDiscard.get(key).equals(this.STARTSWITH) && templateName.startsWith(key.toLowerCase())){
                return true;
            } else if (this.templateDiscard.get(key).equals(this.EQUALS) && templateName.equals(key.toLowerCase())){
                return true;
            }
        }
        return false;
    }





}
