package phi3zh.dataconverter.cleaner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.backoff.BackoffFactory;
import phi3zh.common.utils.Kafka;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import phi3zh.common.utils.backoff.BackoffType;
import phi3zh.config.Wikitext2HtmlConfig;
import phi3zh.dataconverter.StreamConverter;
import scala.concurrent.impl.FutureConvertersImpl;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Wikitext2Html extends StreamConverter<List<Pair<String, String>>> {
    private static final String apiUrl = "https://zh.wikipedia.org/w/api.php";

    Consumer consumer;
    Producer producer;
    String sourceTopic;
    String targetTopic;
    String resourceSemaphoreName;
    String endBucketName;
    String kafkaServer;
    String groupId;
    int pollNum;
    String redisServer;
    Config redisConfig;
    RedissonClient redissonClient;

    public Wikitext2Html(Wikitext2HtmlConfig config){
        super(config.redisConfig(), config.endBucketName());
        this.sourceTopic = config.sourceTopic();
        this.targetTopic = config.targetTopic();
        this.resourceSemaphoreName = config.resourceSemaphoreName();
        this.endBucketName = config.endBucketName();
        this.redisServer = config.redisServer();
        this.kafkaServer = config.kafkaServer();
        this.groupId = config.groupId();
        this.pollNum = config.pollNum();

        this.consumer = Kafka.getStringConsumer(kafkaServer, groupId, Collections.singletonList(sourceTopic), pollNum);
        this.producer = Kafka.getStringProducer(kafkaServer);
        this.redisConfig = config.redisConfig();

        redissonClient = Redisson.create(redisConfig);
    }

    @Override
    protected List<Pair<String, String>> load(){
        ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(5));
        int releaseCount = records.count();
        redissonClient.getSemaphore(resourceSemaphoreName).release(releaseCount);
        return StreamSupport.stream(records.spliterator(), false)
                .map(record->Pair.of(record.key(), record.value())).collect(Collectors.toList());
    }

    @Override
    protected List<Pair<String, String>> process(List<Pair<String, String>> data){
        return  data.stream().flatMap(elem->{
            String title = elem.getLeft();
            String page = elem.getRight();
            List<Pair<String, String>> res = new ArrayList<>();
            try {
                WikitextToHtmlCallable wikitextToHtmlCallable = new WikitextToHtmlCallable();
                wikitextToHtmlCallable.setText(page);
                String htmlPage = BackoffFactory.get(BackoffType.NET_EXP_BACKOFF).wrap(wikitextToHtmlCallable);
                res.add(Pair.of(title, htmlPage));
            } catch (Throwable e){
                e.printStackTrace();
            }
            return res.stream();
        }).collect(Collectors.toList());
    }

    @Override
    protected void save(List<Pair<String, String>> data){
        data.stream().forEach(elem -> {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.targetTopic, elem.getLeft(), elem.getRight());
            producer.send(producerRecord);
        });
    }

    @Override
    protected void shutdown(){
        consumer.close();
        producer.close();
        redissonClient.shutdown();
    }

    private class WikitextToHtmlCallable implements Callable<String> {
        private String textNeedTransfer;
        public WikitextToHtmlCallable(){}

        public void setText(String page){
            this.textNeedTransfer = page;
        }

        @Override
        public String call() throws Exception{
            return wikitextToHtml(this.textNeedTransfer);
        }
    }

    /**
     * transfer wikitext to html page and transfer the html to markdown format
     * @param
     * @return
     * @throws Exception
     */
    public String wikitextToHtml(String page) throws Exception{
        String htmlPage = "";
        try (CloseableHttpClient client = HttpClients.createDefault()){
            HttpPost httpPost = new HttpPost(Wikitext2Html.apiUrl);
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
                    }
                } else {
                    htmlPage = htmlPageJson
                            .get("parse").getAsJsonObject()
                            .get("text").getAsJsonObject()
                            .get("*").getAsString();
                    htmlPage = htmlPage.replace("\n", "");
                }
            }
            return htmlPage;
        } catch (Exception e){
            throw e;
        }
    }

}
