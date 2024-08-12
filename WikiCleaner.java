/*
* WikiCleaner Class to get the dataset of wikiDataPath;
*
* Version: v0.0.1
* */

package phi3zh.datacleaner;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.*;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;

import com.github.houbb.opencc4j.util.ZhConverterUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.stream.StreamSupport;

import org.springframework.beans.factory.annotation.Autowired;
import phi3zh.common.utils.Backoff;
import phi3zh.common.utils.Kafka;
import scala.reflect.ClassTag$;

public class WikiCleaner extends AbstractCleaner<Pair<String, String>> implements Serializable{

    //private static final String apiUrl = "https://zh.wikipedia.org/api/rest_v1/transform/wikitext/to/html";
    private static final String apiUrl = "https://zh.wikipedia.org/w/api.php";
    private static final String pageViewApiUrl = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/zh.wikipedia.org/all-access/user/%s/monthly/%s/%s";
    // the flags
    private static final int STARTSWITH = 1;
    private static final int EQUALS = 2;
    private static final int CONTAINS = 3;

    private static final String ORDER = "order";
    private static final String UNORDER = "unorder";
    private static final String NOTLIST = "notList";

    private static final String NEWLINETAG = "[newLine]";

    // precompile regex pattern
    private static final Pattern BRACKET_PATTERN = Pattern.compile("\\[\\[|\\]\\]|\\{\\{|\\}\\}");
    private static final Pattern CHAPTER_PATTERN = Pattern.compile("=(=+)([^=]*?)=+=");
    private static final Pattern CHAPTER_HTML_PATTERN = Pattern.compile("#(#+) .*");
    private static final Pattern TABLE_BEGIN_PATTERN = Pattern.compile("ul|ol");
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{.*?\\}\\}");
    private static final Pattern REDIRECT_PATTERN = Pattern.compile("#redirect|#重定向");

    // html tag
    private static final String BOLD = "b";
    private static final String ITALIC = "i";
    private static final String HEADER = "h";
    private static final String MULTI_CODE = "syntaxhighlight";
    private static final String SINGLE_CODE = "code";
    private static final List<String> NEWLINES = Stream.of(new String[]{
            "section", "p"
    }).collect(Collectors.toList());

    // name of the save directories
    private static final String CLEANED_CORPUS = "cleaned_corpus";
    private static final String CLEANED_WIKITEXT = "cleaned_wikitext";
    private static final String ERROR_LOGS = "error_logs";
    private static final String SOURCE_WIKITEXT = "source_wikitext";

    boolean isProduceEnd = false;

    // the deprecate template and link in wikipedia
    private static final Map<String, Integer> templateDiscard = Stream.of(new Object[][]{
            {"cite", STARTSWITH},
            {"Main", STARTSWITH},
            {"ref", STARTSWITH},
            {"reflist", EQUALS},
            {"see also", EQUALS},
            {"Wikisource further reading", EQUALS},
            {"Wiktionary", EQUALS},
            {"Commonscat", EQUALS},
            {"wayback", EQUALS},
            {"NoteTag", EQUALS},
            {"CJK-New-Char", EQUALS},
            {"-", EQUALS},
            {"STEM", EQUALS},
            {"Authority control", EQUALS},
            {"efn", EQUALS},
            {"r", EQUALS},
            {"otheruses", EQUALS},
            {"Redirect", STARTSWITH},
            {"not", EQUALS},
            {"About", EQUALS},
            {"sidebar", CONTAINS},
            {"navigation", CONTAINS},
            {"学科", CONTAINS},
            {"侧边栏", CONTAINS},
            {"link", CONTAINS},
    }).collect(Collectors.toMap(data-> (String) data[0], data-> (Integer)data[1]));

    private static final Map<String, Integer> templateConserveStart = Stream.of(new Object[][]{
            {"NoteTA", EQUALS}
    }).collect(Collectors.toMap(data-> (String) data[0], data->(Integer) data[1]));

    private static final Map<String, Integer> linkDiscard = Stream.of(new Object[][]{
            {"file:", STARTSWITH},
            {"category:", STARTSWITH},
    }).collect(Collectors.toMap(data->(String)data[0], data->(Integer)data[1]));

    private static final List<String> tagDiscard = Stream.of(new String[]{
        "meta", "title", "img", "style", "annotation", "table"
    }).collect(Collectors.toList());

    private static final List<String> classAttrDiscard = Stream.of(new String[]{
            "ambox", "noteTA-group", "infobox"
    }).collect(Collectors.toList());

    private static final Stream<ImmutableTriple<String, String, String>> tagAttrDiscardStream = Stream.of(
            ImmutableTriple.of("table", "style", "border:1px solid #ddd; text-align:center; margin: auto;")
    );

    private static final Map<String, Map<String, List<String>>> tagAttrDiscard = tagAttrDiscardStream.collect(
            Collectors.groupingBy(
                    ImmutableTriple::getLeft,
                    Collectors.groupingBy(
                            ImmutableTriple::getMiddle,
                            Collectors.mapping(ImmutableTriple::getRight, Collectors.toList())
                    )
            )
    );

    private static final Map<String, Integer> titleDiscard = Stream.of(new Object[][]{
            {"参见", CONTAINS},
            {"参考", CONTAINS},
            {"参看", EQUALS},
            {"参阅", CONTAINS},
            {"另见", EQUALS},
            {"链接", CONTAINS},
            {"连接", CONTAINS},
            {"连结", CONTAINS},
            {"扩展阅读", EQUALS},
            {"延伸阅读", EQUALS},
            {"引用", CONTAINS},
            {"注释", CONTAINS}
    }).collect(Collectors.toMap(data->(String) data[0], data->(Integer)data[1]));

    // configuration settings, with default values
    private int consumerPollNums;
    private int backoffMaxRetry;
    private int viewThreshold = 10000; // it means that in past year the article should be viewed at least accessThreshold times.
    private int lengthThreshold = 500;
    private int maxKafkaDataSize = 100;
    private int maxConsumeRetryTime = 6;// the max retry number that the conumser that requests.

    private String bootstrapServers;
    private String topicName;
    private SparkSession sparkSession;
    private SparkContext sparkContext;
    private boolean useCache;
    private boolean enableHighQualDetection;
    private String wikiDataPath; // the source datapath of the wikipedia
    private String outputDir; // the output file path of the output
    private String groupId = "wiki";
    public Dataset<Row> data; // the wikipedia dataset
    private transient AdminClient adminClient;
    private transient Consumer<String, String> consumer;

    private static final Logger infoLogger = LogManager.getLogger("WikiInfoLogger");
    private static final Logger errorLogger = LogManager.getLogger("WikiErrorLogger");

    public WikiCleaner(String wikiDataPath,
                       String outputDir,
                       String topicName,
                       String bootstrapServers,
                       int consumerPollNums,
                       int backoffMaxRetry,
                       boolean useCache,
                       boolean enableHighQualDetection){
        this.wikiDataPath = wikiDataPath;
        this.outputDir = outputDir;
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
        this.consumerPollNums = consumerPollNums;
        this.backoffMaxRetry = backoffMaxRetry;
        this.useCache = useCache;
        this.enableHighQualDetection = enableHighQualDetection;
        System.out.println("using cache:" + useCache);
        // load the dataset
        Initialize();
    }

    @Override
    protected void produceElements(){
        Broadcast<String> broadcastBootstarpServers = sparkContext.broadcast(this.bootstrapServers,
                ClassTag$.MODULE$.apply(String.class));
        Broadcast<Boolean> broadcastUseCache = sparkContext.broadcast(this.useCache,
                ClassTag$.MODULE$.apply(Boolean.class));

        System.out.println("for each start");
        this.data.foreachPartition(iterator -> {
            System.out.println("123456");
            // for each partition, we create a Kafka admin
            Properties adminConfig = new Properties();
            adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broadcastBootstarpServers.value());
            AdminClient admin = AdminClient.create(adminConfig);
            Properties producerProperties = new Properties();
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broadcastBootstarpServers.value());
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    StringSerializer.class.getName());
            Producer<String, String> producer = new KafkaProducer<>(producerProperties);
            while (iterator.hasNext()){
                System.out.println("producing has net");
                Row row = iterator.next();
                String title = row.getAs("title");
                String content = ((Row) row.getAs("text")).getAs("_VALUE");
                test();
                // remove the 'wikipedia:' 'help:' pages and redirect pages
//                if (title != null && content != null && !title.contains(":") &&
//                        !isRedirectPage(content) ){
////                    // conver title and content to simple chinese
////                    try {
////                        System.out.println(this.useCache);
////                        if (!this.useCache || Files.notExists(Paths.get(this.outputDir, this.CLEANED_CORPUS, titleToFileName(title)))){
////                            String cleanedContent = cleanWikitext(content);
////                            if (this.enableHighQualDetection && isHighQualText(title, content) || !this.enableHighQualDetection){
////                                Files.write(Paths.get(this.outputDir, this.SOURCE_WIKITEXT, titleToFileName(title)),
////                                        content.getBytes(StandardCharsets.UTF_8));
////                                Files.write(Paths.get(this.outputDir, this.CLEANED_WIKITEXT, titleToFileName(title)),
////                                        cleanedContent.getBytes(StandardCharsets.UTF_8));
////                                infoLogger.info(String.format("add page %s to Kafka", title));
////                                ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, title, cleanedContent);
////                                LocalDateTime now = LocalDateTime.now();
////                                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
////                                String formattedDateTime = now.format(formatter);
////                                // if the size is larger than the threshold, then wait it until it less than threshold
////                                long count = Kafka.dataSizeUnConsume(admin, this.topicName, this.groupId);
////                                while ( count > this.maxKafkaDataSize){
////                                    TimeUnit.SECONDS.sleep(10);
////                                }
////                                System.out.println(formattedDateTime + "  There are " + count + " data in kafka");
////                                producer.send(record);
////                            }
////                            else {
////                                infoLogger.info(String.format("ignore page %s for its low quality", title));
////                            }
////                        }
//////                        else {
//////                            infoLogger.info(String.format("ignore page %s for it has been in output directory", title));
//////                        }
////                    } catch (Exception e){
////                        writeErrorLog(title, e);
////                    }
//                }
//                else {
//                    infoLogger.info(String.format("ignore page %s for it is not the target page that should be consider.", title));
//                }
            }
//            // release the producer and admin
            producer.close();
            admin.close();
        });
        System.out.println("for each end");
        this.isProduceEnd = true;
    }

    private void test(){
        System.out.println("it is a test function");
    }

    /**
     * Consume the elemnents in the message queue, post the content to wikipedia and get the final outcome
     * @return
     */
    @Override
    protected void consumeElements(){
//        ConsumerIterator consumerIterator = new ConsumerIterator();
//        while (consumerIterator.hasNext()){
//            // get the data
//            Collection<Pair<String, String>> elements = consumerIterator.next();
//            // transfer the wikitext
//            List<Pair<String, String>> wikiHtmls = elements.parallelStream().map((elem) ->{
//                try {
////                    WikitextToHtmlCallable wikitextToHtmlCallable = new WikitextToHtmlCallable(elem);
////                    Pair<String, String> htmlPage = Backoff.netExpBackoff(wikitextToHtmlCallable, this.backoffMaxRetry);
//                    Pair<String, String> htmlPage = wikitextToHtml(elem);
//                    String cleanedPage = ZhConverterUtil.toSimple(cleanWikiHtml(htmlPage.getRight()));
//                    return Pair.of(htmlPage.getLeft(), cleanedPage);
//                } catch (Throwable e){
//                    String title = elem.getLeft();
//                    writeErrorLog(title, e);
//                }
//                return Pair.of("", "");
//            }).collect(Collectors.toList());
//
//            // save elements
//            wikiHtmls.parallelStream().forEach(elem -> {
//                try {
//                    if (elem.getRight().trim().length() > 0){
//                        Files.write(Paths.get(this.outputDir, this.CLEANED_CORPUS, titleToFileName(elem.getLeft())),
//                                elem.getRight().getBytes(StandardCharsets.UTF_8));
//                    }
//                    this.infoLogger.info(String.format("save page %s to file", elem.getLeft()));
//                } catch (Exception e){
//                    e.printStackTrace();
//                    System.exit(1);
//                }
//            });
//        }
    }

    /**
     * Initialize needed components, including sparksession and create Kafka message queue
     */
    private void Initialize(){
        // load spark only if no title file input
        this.sparkSession = SparkSession.builder()
                .appName("wikiCleaner")
                .config("spark.master", "local")
                .getOrCreate();
        this.sparkContext = sparkSession.sparkContext();
        Dataset<Row> data = sparkSession.read()
                .format("xml")
                .option("rowTag", "page")
                .load(this.wikiDataPath);
        this.data = data.select("title", "revision.text");
        createKafka();

        // create directories
        Path cleanedCorpusDir = Paths.get(this.outputDir, this.CLEANED_CORPUS);
        Path cleanedWikitextDir = Paths.get(this.outputDir, this.CLEANED_WIKITEXT);
        Path errorPagesDir = Paths.get(this.outputDir, this.ERROR_LOGS);
        Path sourceWikitextDir = Paths.get(this.outputDir, this.SOURCE_WIKITEXT);
        try {
            if (Files.notExists(cleanedCorpusDir)){
                Files.createDirectories(cleanedCorpusDir);
            }
            if (Files.notExists(cleanedWikitextDir)) {
                Files.createDirectories(cleanedWikitextDir);
            }
            if (Files.notExists(errorPagesDir)){
                Files.createDirectories(errorPagesDir);
            }
            if (Files.notExists(sourceWikitextDir)){
                Files.createDirectories(sourceWikitextDir);
            }
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

//    private class GetViewNumCallable implements Callable<Integer>{
//        private String title;
//        public GetViewNumCallable(String title){
//            this.title = title;
//        }
//
//        @Override
//        public Integer call() throws Exception{
//            return getViewNum(this.title);
//        }
//    }

    /**
     * get the view number in the past year
     * @param title
     * @return
     * @throws Exception
     */
    private int getViewNum(String title) throws Exception{
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYYMMdd");
        String rightNowDate = LocalDate.now().format(formatter);
        String lastYearDate = LocalDate.now().minusYears(1).format(formatter);
        title = URLEncoder.encode(title, "utf-8");
        String accessURL = String.format("https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/zh.wikipedia.org/all-access/user/%s/monthly/%s/%s",
                title, lastYearDate, rightNowDate);
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(accessURL);
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == 200){
                String jsonResponse = EntityUtils.toString(response.getEntity());
                JsonArray results = JsonParser.parseString(jsonResponse).getAsJsonObject().getAsJsonArray("items");
                int totalViews = StreamSupport.stream(results.spliterator(), true)
                        .mapToInt(item->item.getAsJsonObject().get("views").getAsInt()).sum();
                return totalViews;
            }
        } catch (Exception e){
            //writeErrorLog(title, e);
            throw e;
        }
        return 0;
    }


//    private class WikitextToHtmlCallable implements Callable<Pair<String, String>>{
//        private Pair<String, String> textNeedTransfer;
//        public WikitextToHtmlCallable(Pair<String, String> textNeedTransfer){
//            this.textNeedTransfer = textNeedTransfer;
//        }
//
//        @Override
//        public Pair<String, String> call() throws Exception{
//            return wikitextToHtml(this.textNeedTransfer);
//        }
//    }

    /**
     * transfer wikitext to html page and transfer the html to markdown format
     * @param elem
     * @return
     * @throws Exception
     */
    public Pair<String, String> wikitextToHtml(Pair<String, String> elem) throws Exception{
        String title = elem.getLeft();
        String page = elem.getRight();
        String htmlPage = "";
        try (CloseableHttpClient client = HttpClients.createDefault()){
            HttpPost httpPost = new HttpPost(WikiCleaner.this.apiUrl);
            JsonObject postBody = new JsonObject();
            postBody.addProperty("action", "parse");
            postBody.addProperty("format", "json");
            postBody.addProperty("contentmodel", "wikitext");
            postBody.addProperty("prop", "text");
            postBody.addProperty("text", page);
            String body = postBody.entrySet().stream().map(
                    entry -> {
                        try {
                            return entry.getKey() + "=" + URLEncoder.encode(entry.getValue().getAsString(), "UTF8");
                        } catch (Exception e){
                            return entry.getKey() + "=" + entry.getValue().getAsString();
                        }
                    }
            ).collect(Collectors.joining("&"));
            httpPost.setEntity(new StringEntity(body, "application/x-www-form-urlencoded", "UTF-8"));

            // post the data to wikipedia
            CloseableHttpResponse response = client.execute(httpPost);
            response.getStatusLine().getStatusCode();
            HttpEntity responseEntity = response.getEntity();

            if (responseEntity != null){
                String htmlPageJsonStr = EntityUtils.toString(responseEntity, StandardCharsets.UTF_8);
                JsonObject htmlPageJson = JsonParser.parseString(htmlPageJsonStr).getAsJsonObject();
                // error process
                if (htmlPageJson.has("error")){
                    if (htmlPageJson.get("error").getAsJsonObject()
                            .get("code").getAsString().equals("ratelimited")){
                    } else {
                        String errorMessage = htmlPageJson.get("error").getAsJsonObject()
                                .get("info").getAsString();
                        errorMessage = "error from wikipedia api: \n" + errorMessage;
                        writeErrorLog(title, errorMessage);
                    }
                } else {
                    htmlPage = htmlPageJson
                            .get("parse").getAsJsonObject()
                            .get("text").getAsJsonObject()
                            .get("*").getAsString();
                    htmlPage = htmlPage.replace("\n", "");
                }
            }
            return Pair.of(title, htmlPage);
        } catch (Exception e){
            writeErrorLog(title, e);
            throw e;
        }
    }

    /**
     * clean the wikitext
     * @param page the wikitext origin files
     * @return
     */
    private String cleanWikitext(String page){
        List<String> removeRegs = new ArrayList<>();
        removeRegs.add("<!--.*?-->"); // comments
        removeRegs.add("(?s)<ref.*?>.*?</ref>|<ref.*?/>"); // ref links
        page = page.replaceAll(String.join("|", removeRegs), "");
        Matcher bracketMatcher = this.BRACKET_PATTERN.matcher(page);
        Deque<Pair<String, Integer>> deque = new ArrayDeque<>();
        Stack<Pair<String, Integer>> refBlockBegin = new Stack<>();
        List<Pair<Integer, Integer>> delIntervals = new ArrayList<>();
        boolean isPageTemplate = true;
        int lastIdx = 0;
        while (bracketMatcher.find()){
            if (bracketMatcher.group().equals("{{") || bracketMatcher.group().equals("[[")){
                deque.addLast(Pair.of(bracketMatcher.group(), bracketMatcher.start()));
            } else {
                Pair<String, Integer> left = deque.pollLast();
                Pair<String, Integer> right = Pair.of(bracketMatcher.group(), bracketMatcher.end());
                if (left == null){
                    continue;
                }

                String content = page.substring(left.getRight(), right.getRight()).toLowerCase();
                if (left.getLeft().equals("[[")){
                    content = content.substring(2, content.length()-2).toLowerCase();
                    if (shouleDeleteLink(content)){
                        delIntervals.add(Pair.of(left.getRight(), right.getRight()));
                    }
                }
                else if (left.getLeft().equals("{{")){
                    String templateName = content
                            .substring(2, content.length()-2)
                            .toLowerCase().split("\\|")[0].trim();
                    // delete the begin pages
                    if (deque.isEmpty() && isPageTemplate){
                        if (page.substring(lastIdx, left.getRight()).replaceAll("\\s+", "").isEmpty()){
                            if (!shouldConserveTemplateInBegin(templateName)){
                                delIntervals.add(Pair.of(left.getRight(), right.getRight()));
                            }
                            lastIdx = right.getRight();
                            continue;
                        } else {
                            isPageTemplate = false;
                        }
                    }
                    if (shouldDeleteTemplate(templateName)){
                        delIntervals.add(Pair.of(left.getRight(), right.getRight()));
                    }
                    // remove ref blocks
                    else if (templateName.toLowerCase().equals("refbegin")){
                        refBlockBegin.push(Pair.of(templateName, left.getRight()));
                    } else if (templateName.toLowerCase().equals("refend")){
                        if (refBlockBegin.size() > 0){
                            Pair<String, Integer> blockBegin = refBlockBegin.pop();
                            delIntervals.add(Pair.of(blockBegin.getRight(), right.getRight()));
                        } else {
                            delIntervals.add(Pair.of(bracketMatcher.start(), bracketMatcher.end()));
                        }

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
        page = res;
        // trim null space and only template line
        String[] stirngLines = res.split("\n");
        res = "";
        for (String line: stirngLines){
            if (line.replaceAll("\\s", "").
                    replaceAll("[\\p{P}&&[^}]]", "").length() > 0
                || !this.TEMPLATE_PATTERN.matcher(line.trim()).matches()){
                res = res + line + "\n";
            }
        }

        // split the res with chapter
        Matcher chapterMatcher = this.CHAPTER_PATTERN.matcher(page);
        page = res;
        res = "";
        int chapterStart = 0;
        int chapterEnd = 0;
        int chapterLevel = 1;
        String chapterTitle = "";
        while (chapterMatcher.find()){
            int curChapterLevel = chapterMatcher.group(1).length();
            // add the chapter content when the length of the chapter is not 0
            if (curChapterLevel >= chapterLevel
                    && page.substring(chapterEnd, chapterMatcher.start()).trim().length() > 0
                    && !shouldDeleteTitle(chapterTitle)){
                res = res + page.substring(chapterStart, chapterMatcher.start());
            }
            chapterTitle = chapterMatcher.group(2);
            chapterStart = chapterMatcher.start();
            chapterEnd = chapterMatcher.end();
            chapterLevel = curChapterLevel;
        }

        if (page.substring(chapterEnd).trim().length() > 0
            && !shouldDeleteTitle(chapterTitle)){
            res = res + page.substring(chapterStart);
        }

        return res;
    }

    /**
     * clean the wiki html text into the markdown format.
     * @param page the html page of the wiki
     * @return the markdown format that has been cleaned
     */
    private String cleanWikiHtml(String page){
        org.jsoup.nodes.Document wikiDoc = Jsoup.parse(page);
        Element root = wikiDoc.root();
        Element node = root;
        boolean forward = true; // denotes it is a forward or backward process
        while (root.childrenSize() > 0) {
            if (forward && !shouldDeleteTag(node)) {
                if (this.TABLE_BEGIN_PATTERN.matcher(node.tagName()).matches()) {
                    node.text(parseList(node));
                    if (node.nextElementSibling() != null) {
                        node = node.nextElementSibling();
                    } else {
                        node = node.parent();
                        forward = false;
                    }
                } else if (node.childrenSize() > 0) {
                    node = node.child(0);
                } else {
                    node.text(transferHtmlToMarkdown(node.tagName(), node.text()));
                    if (node.nextElementSibling() != null) {
                        node = node.nextElementSibling();
                    } else {
                        node = node.parent();
                        forward = false;
                    }
                }
            } else if (!forward) {
                node.html(transferHtmlToMarkdown(node.tagName(), node.text()));
                if (node.nextElementSibling() != null) {
                    forward = true;
                    node = node.nextElementSibling();
                } else {
                    node = node.parent();
                }
            } else {
                Element tmp;
                if (node.nextElementSibling() != null) {
                    tmp = node.nextElementSibling();
                } else {
                    tmp = node.parent();
                    forward = false;
                }
                node.remove();
                node = tmp;
            }
        }

        String res = root.text().replaceAll(
                this.NEWLINETAG.replace("[", "\\[").replace("]", "\\]"),
                "\n").replace("[编辑]", "");

        page = res;
        res = "";
        Matcher chapterMat = this.CHAPTER_HTML_PATTERN.matcher(page);
        int chapterStart = 0;
        int chapterEnd = 0;
        while (chapterMat.find()){
            if (page.substring(chapterEnd, chapterMat.start()).trim().length() > 0){
                res = res + page.substring(chapterStart, chapterMat.start());
            }
            chapterStart = chapterMat.start();
            chapterEnd = chapterMat.end();
        }
        if (page.substring(chapterEnd).trim().length() > 0){
            res = res + page.substring(chapterStart);
        }

        // delete the null lines
        String tmp = res;
        res = "";
        boolean isStart = true;
        int spaceLineCount = 0;
        for (String line: tmp.split("\n")){
            if (line.trim().length() > 0){
                isStart = false;
                spaceLineCount = 0;
                res += line + "\n";
            } else if (!isStart && spaceLineCount == 0){
                res += "\n";
                spaceLineCount ++;
            }
        }
        return res;
    }

    /**
     * @param content
     * @return
     */
    private String transferHtmlToMarkdown(String tagName, String content){
        if (tagName.equals(this.BOLD)){
            return "**" + content + "**";
        } else if (tagName.equals(this.ITALIC)){
            return "*" + content + "*";
        } else if (tagName.equals(this.MULTI_CODE)){
            return "```" + content + "```";
        } else if (tagName.equals(this.SINGLE_CODE)){
            return "`" + content + "`";
        }
        // if it is a header
        else if (tagName.length() == 2 && tagName.substring(0, 1).equals(this.HEADER) &&
                Character.isDigit(tagName.charAt(1))){
            int headerLevel = Integer.parseInt(tagName.substring(1,2));
            StringBuilder resBuilder = new StringBuilder();
            resBuilder.append(this.NEWLINETAG);
            resBuilder.append(this.NEWLINETAG);
            for (int i = 0; i < headerLevel; i++){
                resBuilder.append("#");
            }
            resBuilder.append(" ");
            resBuilder.append(content);
            return resBuilder.toString();
        } else if (this.NEWLINES.contains(tagName)){
            return this.NEWLINETAG + content;
        }
        else {
            return content;
        }
    }

    private String titleToFileName(String title){
        title = title.replaceAll("<|>|:|\"|/|\\|\\||\\?|\\*", "_");
        return title + ".txt";
    }

    // assert the link should be delete or not
    private boolean shouleDeleteLink(String link){
        for (String key: this.linkDiscard.keySet()){
            if (this.linkDiscard.get(key).equals(this.STARTSWITH) && link.startsWith(key.toLowerCase())){
                return true;
            }
            else if (this.linkDiscard.get(key).equals(this.EQUALS) && link.equals(key.toLowerCase())){
                return true;
            } else if (this.linkDiscard.get(key).equals(this.CONTAINS) && link.contains(key.toLowerCase())){
                return true;
            }
        }
        return false;
    }

    // assert the template shoulb be delete or not
    private boolean shouldDeleteTemplate(String templateName){
        for (String key: this.templateDiscard.keySet()){
            if (this.templateDiscard.get(key).equals(this.STARTSWITH) && templateName.startsWith(key.toLowerCase())){
                return true;
            } else if (this.templateDiscard.get(key).equals(this.EQUALS) && templateName.equals(key.toLowerCase())){
                return true;
            } else if (this.templateDiscard.get(key).equals(this.CONTAINS) && templateName.contains(key.toLowerCase())){
                return true;
            }
        }
        return false;
    }

    // assert the template in the begin of the text should be conserver or not
    private boolean shouldConserveTemplateInBegin(String templateName){
        for (String key: this.templateConserveStart.keySet()){
            if (this.templateConserveStart.get(key).equals(this.STARTSWITH) && templateName.startsWith(key.toLowerCase())){
                return true;
            } else if (this.templateConserveStart.get(key).equals(this.EQUALS) && templateName.equals(key.toLowerCase())){
                return true;
            } else if (this.templateConserveStart.get(key).equals(this.CONTAINS) && templateName.contains(key.toLowerCase())){
                return true;
            }
        }
        return false;
    }

    // assert the html tag should be delete or not
    private boolean shouldDeleteTag(Element node){
        // discard the tag
        if (this.tagDiscard.contains(node.tagName())){
            return true;
        }

        if (this.tagAttrDiscard.containsKey(node.tagName())){
            Map<String, List<String>> tmp = this.tagAttrDiscard.get(node.tagName());
            for (String attrName: tmp.keySet()){
                if (node.hasAttr(attrName)){
                    for (String attr: tmp.get(attrName)){
                        if (attr.equals(node.attr(attrName))){
                            return true;
                        }
                    }
                }
            }
        }

        for (String attr: this.classAttrDiscard){
            if (node.hasClass(attr)){
                return true;
            }
        }
        return false;
    }

    // assert the title should be delete or not
    private boolean shouldDeleteTitle(String title){
        title = ZhConverterUtil.toSimple(title.trim());
        for (String key: this.titleDiscard.keySet()){
            if (this.titleDiscard.get(key).equals(this.CONTAINS)
                    && title.contains(key)){
                return true;
            } else if (this.titleDiscard.get(key).equals(this.EQUALS)
                && title.equals(key)){
                return true;
            }
        }
        return false;
    }

    private boolean isRedirectPage(String page){
        if (this.REDIRECT_PATTERN.matcher(page.toLowerCase()).find()){
            return true;
        } else {
            return false;
        }
    }

    private String parseList(Element table){
        return parseList(table, 0) + this.NEWLINETAG;
    }

    private String parseList(Element table, int depth){
        int seq = 1;
        String listType = getListType(table.tagName());
        if (table.childrenSize() == 0){
            return "";
        }
        Element elem = table.child(0);
        StringBuilder resBuilder = new StringBuilder();
        while (elem !=null){
            if (elem.childrenSize() == 0 || !this.TABLE_BEGIN_PATTERN.matcher(elem.child(0).tagName()).matches()){
                StringBuilder tmpBuilder = new StringBuilder();
                tmpBuilder.append(this.NEWLINETAG);
                for (int i = 0; i < depth; i ++){
                    tmpBuilder.append("    ");
                }
                if (listType.equals(this.ORDER)){
                    tmpBuilder.append(String.format("%d. %s", seq, elem.text()));
                    seq += 1;
                } else if (listType.equals(this.UNORDER)){
                    tmpBuilder.append(String.format("- %s", elem.text()));
                }
                resBuilder.append(tmpBuilder);
            }
            else{
                Element child = elem.child(0);
                resBuilder.append(parseList(child, depth + 1));
            }
            elem = elem.nextElementSibling();
        }
        return resBuilder.toString();
    }

    private String getListType(String tagName){
        if (tagName.equals("ol")){
            return this.ORDER;
        } else if (tagName.equals("ul")){
            return this.UNORDER;
        } else {
            return this.NOTLIST;
        }
    }

    private boolean isHighQualText(String title, String text){
        System.out.println("Asserting high quality");
        try {
//            GetViewNumCallable getViewNumCallable = new GetViewNumCallable(title);
//            int totalView = Backoff.netExpBackoff(getViewNumCallable, this.backoffMaxRetry);
            int totalView = getViewNum(title);
            System.out.println("view num:" + totalView);
            if (totalView < viewThreshold){
                return false;
            }
            int textLength = text.replaceAll("\\s", "").length();
            if (textLength < lengthThreshold){
                return false;
            }
            System.out.println(totalView);
            return true;
        } catch (Throwable e){
            e.printStackTrace();
            System.exit(1);
        }

        return false;
    }

    /***
     * create the Kafka message topics
     */
    private void createKafka(){
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        this.adminClient = AdminClient.create(adminConfig);
        try {
            // if the topics not in the Kafka, then create it, if has delete and create it
            if (adminClient.listTopics().names().get().contains(this.topicName)){
                adminClient.deleteTopics(Collections.singletonList(this.topicName)).all().get();
            }
            adminClient.createTopics(Collections.singletonList(new NewTopic(this.topicName, 1, (short) 1))).all().get();

            // initialize consumer
            Properties consumerProp = new Properties();
            consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
            consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProp.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.consumerPollNums);
            consumerProp.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            consumerProp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<>(consumerProp);
            this.consumer.subscribe(Collections.singletonList(this.topicName));
        } catch (Exception e){
            e.printStackTrace();
            System.exit(1);
        }
    }

    // write error message and its source format into error message
    private void writeErrorLog(String title, Exception e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String errorMessage = String.format("Error occur while processing %s: \n", title) + sw;
        this.errorLogger.error(errorMessage);
    }

    private void writeErrorLog(String title, Throwable e){
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        String errorMessage = String.format("Error occur while processing %s: \n", title) + sw;
        this.errorLogger.error(errorMessage);
    }

    private void writeErrorLog(String title, String errorMessage){
        errorMessage = String.format("Error occur while processing %s:\n", title) + errorMessage;
        this.errorLogger.error(errorMessage);
    }

    private class ConsumerIterator implements Iterator<Collection<Pair<String, String>>>{

        List<Pair<String, String>> cache = new ArrayList<>();

        // extract the data first then return the hasNext, or if it runs with multiple processes, it will encounter
        // many backwards
        @Override
        public boolean hasNext(){
            int retryTime = 0;
            this.cache.clear();
            while (retryTime < WikiCleaner.this.maxConsumeRetryTime || !WikiCleaner.this.isProduceEnd){
                ConsumerRecords<String, String> records = WikiCleaner.this.consumer.poll(Duration.ofSeconds(5));
                if (records.count() == 0){
                    retryTime ++;
                    // wait 10 seconds for next retry
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (Exception e){
                        e.printStackTrace();
                        System.exit(1);
                    }
                } else {
                    System.out.println("There are " + records.count() + " data in consumer" );
                    this.cache = StreamSupport.stream(records.spliterator(), false)
                            .map(record->Pair.of(record.key(), record.value()))
                            .collect(Collectors.toList());
                    return true;
                }
            }
            return false;
        }

        @Override
        public Collection<Pair<String, String>> next(){
            return this.cache;
        }
    }
}
