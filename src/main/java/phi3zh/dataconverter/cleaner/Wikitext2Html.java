package phi3zh.dataconverter.cleaner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.backoff.BackoffFactory;
import phi3zh.common.utils.Kafka;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import phi3zh.common.utils.backoff.BackoffType;
import phi3zh.config.Wikitext2HtmlConfig;
import phi3zh.dataconverter.Converter;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Wikitext2Html implements Converter {
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
    List<String> redisServers;
    Config redisConfig;

    public Wikitext2Html(String loadTopic,
                         String saveTopic,
                         String bootstrapServers,
                         String groupId,
                         int pollNum){
        this.sourceTopic = loadTopic;
        this.targetTopic = saveTopic;
        this.consumer = Kafka.getStringConsumer(bootstrapServers, groupId, Collections.singletonList(loadTopic), pollNum);
        this.producer = Kafka.getStringProducer(bootstrapServers);

    }

    public Wikitext2Html(Wikitext2HtmlConfig config){
        this.sourceTopic = config.sourceTopic();
        this.targetTopic = config.targetTopic();
        this.resourceSemaphoreName = config.resourceSemaphoreName();
        this.endBucketName = config.endBucketName();
        this.redisServers = config.redisServers();
        this.kafkaServer = config.kafkaServer();
        this.groupId = config.groupId();
        this.pollNum = config.pollNum();

        this.consumer = Kafka.getStringConsumer(kafkaServer, groupId, Collections.singletonList(sourceTopic), pollNum);
        this.producer = Kafka.getStringProducer(kafkaServer);
        this.redisConfig = new Config();
        redisServers.stream().parallel().forEach(elem->redisConfig.useSingleServer().setAddress(elem));
    }

    @Override
    public void run(){
        RedissonClient client = Redisson.create(this.redisConfig);
        while(!(Boolean)client.getBucket(endBucketName).get()){
            ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofSeconds(5)); // load data from kafka
            int releaseCount = records.count();
            client.getSemaphore(resourceSemaphoreName).release(releaseCount);
            StreamSupport.stream(records.spliterator(), true)
                    .forEach(record -> {
                        String title = record.key();
                        String page = record.value();
                        try {
                            WikitextToHtmlCallable wikitextToHtmlCallable = new WikitextToHtmlCallable();
                            wikitextToHtmlCallable.setText(page);
                            String htmlPage = BackoffFactory.get(BackoffType.NET_EXP_BACKOFF).wrap(wikitextToHtmlCallable);
                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.targetTopic, title, htmlPage);
                            producer.send(producerRecord);
                            System.out.println("produce element");
                            System.out.println(targetTopic);
                        } catch (Throwable e){
                            e.printStackTrace();
                        }
                    });
        }
        client.shutdown();
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
