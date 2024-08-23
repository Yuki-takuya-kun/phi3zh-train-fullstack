package phi3zh.dataconverter.controller;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RSemaphore;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.Kafka;
import phi3zh.config.CommonConfig;
import phi3zh.config.WikihtmlCleanerConfig;
import phi3zh.config.Wikitext2HtmlConfig;
import phi3zh.config.WikitextCleanerConfig;
import phi3zh.dataconverter.cleaner.WikihtmlCleaner;
import phi3zh.dataconverter.cleaner.Wikitext2Html;
import phi3zh.dataconverter.cleaner.WikitextCleaner;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WikiController extends Controller{

    private static final String text2HtmlTopic = "topic_wiki_text2html";
    private static final String htmlCleanTopic = "topic_wiki_htmlClean";

    private static final String text2htmlResource = "resource_wiki_text2html";

    private static final String text2htmlEndBucketName = "bucket_end_text2html";
    private static final String htmlCleanerEndBucketName = "bucket_end_htmlCleaner";

    protected List<Triple<String, Integer, Short>> kafkaTopics = Stream.of(new Object[][]{
            {text2HtmlTopic, 1, (short) 1}
    }).map(elem->Triple.of((String)elem[0], (Integer)elem[1], (Short)elem[2])).collect(Collectors.toList());

    protected List<Pair<String, Boolean>> booleanBucketWithDefaults = Stream.of(new Object[][]{
            {text2htmlEndBucketName, false},
            {htmlCleanerEndBucketName, false}
    }).map(elem->Pair.of((String)elem[0], (Boolean)elem[1])).collect(Collectors.toList());

    protected List<Pair<String, Integer>> semaphoreWithDefaultValues = Stream.of(new Object[][]{
            {text2htmlResource, 100}
    }).map(elem->Pair.of((String)elem[0], (Integer)elem[1])).collect(Collectors.toList());

    List<String> redisServers;

    CommonConfig commonConfig;
    WikitextCleanerConfig wikitextCleanerConfig;
    Wikitext2HtmlConfig wikitext2HtmlConfig;
    WikihtmlCleanerConfig wikihtmlCleanerConfig;

    WikitextCleaner wikitextCleaner;
    Wikitext2Html wikitext2Html;
    WikihtmlCleaner wikihtmlCleaner;

    public WikiController(){
        commonConfig = new CommonConfig();
        this.redisServers = commonConfig.redisServers();

        boolean useCache = false;
        boolean enableHighQualDetection = false;
        wikitextCleanerConfig = new WikitextCleanerConfig(
                "E:\\Datasets\\phi3-zh\\source_dataset\\test.xml",
                "E:/Datasets/phi3-zh/output",
                text2HtmlTopic,
                text2htmlResource,
                useCache,
                enableHighQualDetection,
                commonConfig
        );
        wikitext2HtmlConfig = new Wikitext2HtmlConfig(
                text2HtmlTopic,
                htmlCleanTopic,
                text2htmlResource,
                text2htmlEndBucketName,
                commonConfig
        );
        wikihtmlCleanerConfig = new WikihtmlCleanerConfig(
                "E:/Datasets/phi3-zh/output",
                htmlCleanTopic,
                "testrouce",
                htmlCleanerEndBucketName,
                commonConfig
        );

        this.wikitextCleaner = new WikitextCleaner(wikitextCleanerConfig);
        this.wikitext2Html = new Wikitext2Html(wikitext2HtmlConfig);
        this.wikihtmlCleaner = new WikihtmlCleaner(wikihtmlCleanerConfig);

    }

    @Override
    public void run(){
        initialize();

        ExecutorService threadPool = Executors.newFixedThreadPool(3);
        Future wikitextCleanerFuture = threadPool.submit(wikitextCleaner);
        Future wikitext2HtmlFuture = threadPool.submit(wikitext2Html);
        Future wikihtmlCleanerFuture = threadPool.submit(wikihtmlCleaner);


        addMonitorAttr(wikitextCleanerFuture, text2htmlEndBucketName, text2HtmlTopic, wikitext2HtmlConfig.groupId());
        addMonitorAttr(wikitext2HtmlFuture, htmlCleanerEndBucketName, htmlCleanTopic, wikihtmlCleanerConfig.groupId());
        addMonitorAttr(wikihtmlCleanerFuture, null, null, null);
        runMonitor();

        threadPool.shutdown();

        shutdown();
    }

    private void initialize(){
        setKafkaTopics(kafkaTopics);
        setSemaphoreWithDefaultValues(semaphoreWithDefaultValues);
        setBooleanBucketWithDefaults(booleanBucketWithDefaults);

        Properties kafkaConfig = new Properties();
        kafkaConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, commonConfig.kafkaServer());
        setKafkaConfig(kafkaConfig);

        Config redisConfig = new Config();
        redisServers.stream().forEach(elem -> redisConfig.useSingleServer().setAddress(elem));
        setRedisConfig(redisConfig);

        initializeKafka();
        initializeRedis();
    }

    private void shutdown(){
        shutdownKafka();
        shutdownRedis();
    }


}
