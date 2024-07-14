import java.io.IOException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpResponse;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.commons.text.StringEscapeUtils;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class test {
    public static String convertWikiTextToHtml(String wikiText) throws IOException {
        String apiUrl = "https://zh.wikipedia.org/w/api.php";  // 替换为你的MediaWiki服务器的API URL

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            // 创建HttpPost对象
            HttpPost post = new HttpPost(apiUrl);

            JsonObject postBody = new JsonObject();
            postBody.addProperty("action", "parse");
            postBody.addProperty("format", "json");
            postBody.addProperty("contentmodel", "wikitext");
            postBody.addProperty("prop", "text");
            postBody.addProperty("text", wikiText);
            // 设置POST请求体
            String body = postBody.entrySet().stream().map(
                    entry -> {
                        try {
                            return entry.getKey() + "=" + URLEncoder.encode(entry.getValue().getAsString(), "UTF8");
                        } catch (Exception e){
                            return entry.getKey() + "=" + entry.getValue().getAsString();
                        }
                    }
            ).collect(Collectors.joining("&"));
            System.out.println(body);
            String postData = "action=parse&format=json&text=" + URLEncoder.encode(wikiText, "UTF-8") + "&contentmodel=wikitext&prop=text";
            post.setEntity(new StringEntity(body, "application/x-www-form-urlencoded", "UTF-8"));

            // 执行POST请求
            HttpResponse response = client.execute(post);

            // 检查响应状态码
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new IOException("Unexpected status code: " + statusCode);
            }

            // 读取响应体内容并返回
            String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");
            JsonObject responseObj = JsonParser.parseString(responseString).getAsJsonObject();
            responseString = responseObj.get("parse").getAsJsonObject()
                    .get("text").getAsJsonObject()
                    .get("*").getAsString();

            return responseString;  // 这里只是返回原始响应。你可能需要解析JSON以提取HTML部分。
        }
    }

    public static void main(String[] args) {
        String text = "a code }.";
        String regex = "[\\p{P}&&[^}]]"; // 匹配所有标点，除了 '}'
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);

        if (!matcher.find()) {
            System.out.println("No punctuation found excluding '}'.");
        } else {
            do {
                System.out.println("Found a punctuation excluding '}': " + matcher.group());
            } while (matcher.find());
        }

    }
}
