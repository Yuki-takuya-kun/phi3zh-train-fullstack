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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import phi3zh.common.utils.backoff.BackoffFactory;
import phi3zh.common.utils.backoff.BackoffType;
import phi3zh.config.Wikitext2HtmlConfig;
import phi3zh.dataconverter.SparkKafkaConverter;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Wikitext2Html extends SparkKafkaConverter {
    private static final String apiUrl = "https://zh.wikipedia.org/w/api.php";

    String sourceTopic;
    String targetTopic;
    String resourceSemaphoreName;
    String endBucketName;
    String kafkaServer;
    String redisServer;
    String kafkaStartingOffset;
    String checkpointLocation;

    public Wikitext2Html(Wikitext2HtmlConfig config){
        super(config.getRedisConfig(), config.getEndBucketName(), config.getSparkAppName(),
                config.getSparkMaster(), config.getKafkaServer(), config.getSourceTopic(),
                config.getKafkaStartingOffset(), config.getResourceSemaphoreName());
        this.sourceTopic = config.getSourceTopic();
        this.targetTopic = config.getTargetTopic();
        this.resourceSemaphoreName = config.getResourceSemaphoreName();
        this.endBucketName = config.getEndBucketName();
        this.redisServer = config.getRedisServer();
        this.kafkaServer = config.getKafkaServer();
        this.kafkaStartingOffset = config.getKafkaStartingOffset();
        this.checkpointLocation = config.getCheckpointLocation();
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> data){
        Dataset<Row> result = data.flatMap(
            (FlatMapFunction<Row, Row>) row -> {
                String title = row.getAs(0);
                String page = row.getAs(1);
                List<Row> res = new ArrayList<>();
                try {
                    WikitextToHtmlCallable wikitextToHtmlCallable = new WikitextToHtmlCallable();
                    wikitextToHtmlCallable.setText(page);
                    String htmlPage = BackoffFactory.get(BackoffType.NET_EXP_BACKOFF).wrap(wikitextToHtmlCallable);
                    res.add(RowFactory.create(title, htmlPage));
                } catch (Throwable e){
                    e.printStackTrace();
                }
                return res.iterator();
            },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("key", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("value", DataTypes.StringType, false, Metadata.empty())
                }))
        );
        return result;
    }

    @Override
    protected StreamingQuery streamingSave(Dataset<Row> data){
        try {
            StreamingQuery streamingQuery = data
                    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
                    .writeStream()
                    .outputMode("append")
                    .format("kafka")
                    .option("kafka.bootstrap.servers", this.kafkaServer)
                    .option("topic", this.targetTopic)
                    .option("checkpointLocation", this.checkpointLocation)
                    .start();
            return streamingQuery;
        } catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
