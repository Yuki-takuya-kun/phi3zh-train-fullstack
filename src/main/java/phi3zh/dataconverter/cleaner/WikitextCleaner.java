package phi3zh.dataconverter.cleaner;

import com.github.houbb.opencc4j.util.ZhConverterUtil;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.checkerframework.checker.units.qual.C;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.backoff.BackoffFactory;
import phi3zh.common.utils.backoff.BackoffType;
import phi3zh.config.WikitextCleanerConfig;
import scala.reflect.ClassTag$;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static phi3zh.common.utils.Kafka.getStringProducer;

public class WikitextCleaner extends Cleaner<String> {

    // the flags
    private static final int STARTSWITH = 1;
    private static final int EQUALS = 2;
    private static final int CONTAINS = 3;

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

    private static final int viewThreshold = 10000; // it means that in past year the article should be viewed at least accessThreshold times.
    private static final int lengthThreshold = 500;

    // name of the save directories
    private static final String CLEANED_CORPUS = "cleaned_corpus";
    private static final String CLEANED_WIKITEXT = "cleaned_wikitext";
    private static final String ERROR_LOGS = "error_logs";
    private static final String SOURCE_WIKITEXT = "source_wikitext";

    String wikiDataPath; // the datapath of the wiki database
    String outputDir; // the output directory
    String topicName; // the topic name in kafka
    String bootstrapServers; // the kafka server address
    String resourceSemaphoreName;
    List<String> redisServers;
    boolean useCache;
    boolean enableHighQualDetection;

    Logger infoLogger = LogManager.getLogger("WikiInfoLogger");

    private Dataset<Row> data;
    private static final SparkSession sparkSession = SparkSession.builder()
            .appName("wikitextCleaner")
            .config("spark.master", "local")
            .getOrCreate();

    // precompile regex pattern
    private static final Pattern BRACKET_PATTERN = Pattern.compile("\\[\\[|\\]\\]|\\{\\{|\\}\\}");
    private static final Pattern CHAPTER_PATTERN = Pattern.compile("=(=+)([^=]*?)=+=");
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\{\\{.*?\\}\\}");
    private static final Pattern REDIRECT_PATTERN = Pattern.compile("#redirect|#重定向");

    public WikitextCleaner(String wikiDataPath,
                           String outpuDir,
                           String topicName,
                           String bootstrapServers,
                           boolean useCache,
                           boolean enableHighQualDetection){
        this.wikiDataPath = wikiDataPath;
        this.outputDir = outpuDir;
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
        this.useCache = useCache;
        this.enableHighQualDetection = enableHighQualDetection;
    }

    public WikitextCleaner(WikitextCleanerConfig config){
        this.wikiDataPath = config.wikidataPath();
        this.outputDir = config.outputDir();
        this.topicName = config.targetTopicName();
        this.bootstrapServers = config.kafkaServer();
        this.resourceSemaphoreName = config.resourceSemaphoreName();
        this.useCache = config.useCache();
        this.enableHighQualDetection = config.enableHighQualDetection();
        this.redisServers = config.redisServers();

    }

    private void load(){
        Dataset<Row> data = sparkSession.read()
                .format("xml")
                .option("rowTag", "page")
                .load(this.wikiDataPath);
        this.data = data.select("title", "revision.text");
    }

    @Override
    public void run(){
        load();
        this.data.foreachPartition(iterator->{
            Config redisConfig = new Config();
            this.redisServers.stream().forEach(elem->redisConfig.useSingleServer().setAddress(elem));
            try (Producer producer = getStringProducer(this.bootstrapServers)){
                RedissonClient redissonClient = Redisson.create(redisConfig);
                while (iterator.hasNext()){
                    //redissonClient.getSemaphore(resourceSemaphoreName).acquire();
                    Row row = iterator.next();
                    String title = row.getAs("title");
                    String content = ((Row) row.getAs("text")).getAs("_VALUE");
                    // remove the 'wikipedia:' 'help:' pages and redirect pages
                    if (!shouldIgnorePage(title, content)){
                        // conver title and content to simple chinese
                                String cleanedContent = clean(content);
                            if (this.enableHighQualDetection && isHighQualText(title, content) || !this.enableHighQualDetection){
                                Files.write(Paths.get(this.outputDir, this.SOURCE_WIKITEXT, titleToFileName(title)),
                                        content.getBytes(StandardCharsets.UTF_8));
                                Files.write(Paths.get(this.outputDir, this.CLEANED_WIKITEXT, titleToFileName(title)),
                                        cleanedContent.getBytes(StandardCharsets.UTF_8));
                                infoLogger.info(String.format("add page %s to Kafka", title));
                                ProducerRecord<String, String> record = new ProducerRecord<>(this.topicName, title, cleanedContent);
                                producer.send(record);
                            }
                            else {
                                infoLogger.info(String.format("ignore page %s for its low quality", title));
                            }
                        }
                    }
                redissonClient.shutdown();
                }
            });
    }

    private boolean isRedirectPage(String page){
        if (this.REDIRECT_PATTERN.matcher(page.toLowerCase()).find()){
            return true;
        } else {
            return false;
        }
    }

    private String titleToFileName(String title){
        title = title.replaceAll("<|>|:|\"|/|\\|\\||\\?|\\*", "_");
        return title + ".txt";
    }

    /**
     * clean the wikitext
     * @param page the wikitext origin files
     * @return
     */
    @Override
    protected String clean(String page){
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

    // assert the page should be conserve according to the title
    private boolean shouldIgnorePage(String title, String content){
        return title == null || title.contains(":") || content == null || isRedirectPage(content) ||
                this.useCache && Files.exists(Paths.get(this.outputDir, this.CLEANED_CORPUS, titleToFileName(title)));
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

    private class GetViewNumCallable implements Callable<Integer> {
        private String title;
        public GetViewNumCallable(String title){
            this.title = title;
        }

        @Override
        public Integer call() throws Exception{
            return getViewNum(this.title);
        }
    }

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

    private boolean isHighQualText(String title, String text){
        try {
            WikitextCleaner.GetViewNumCallable getViewNumCallable = new WikitextCleaner.GetViewNumCallable(title);
            int totalView = BackoffFactory.get(BackoffType.NET_EXP_BACKOFF).wrap(getViewNumCallable);
            //int totalView = getViewNum(title);
            if (totalView < viewThreshold){
                return false;
            }
            int textLength = text.replaceAll("\\s", "").length();
            if (textLength < lengthThreshold){
                return false;
            }
            return true;
        } catch (Throwable e){
            e.printStackTrace();
            System.exit(1);
        }

        return false;
    }



}
