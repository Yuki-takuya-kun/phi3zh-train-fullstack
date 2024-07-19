import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import com.google.gson.Gson;

import java.io.IOException;

public class test {

    public static void main(String[] args) {
        String articleTitle = "Albert_Einstein";
        String startDate = "20240505";  // 开始日期
        String endDate = "20240731";    // 结束日期
        String jsonResult = getWikipediaViews(articleTitle, startDate, endDate);
        JsonElement jsonElement = JsonParser.parseString(jsonResult);
        JsonObject jsonObject = jsonElement.getAsJsonObject();
        if (jsonResult != null) {
            Gson gson = new Gson();
            PageViewsData data = gson.fromJson(jsonResult, PageViewsData.class);
            System.out.println("Views data: " + data);
        }
    }

    public static String getWikipediaViews(String articleTitle, String start, String end) {
//        String url = String.format(
//                "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/user/%s/%s/%s",
//                articleTitle, start, end);
        String url = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/user/Tiger/monthly/20240101/20240201";

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpGet request = new HttpGet(url);
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                System.out.println(response.getEntity());
                String jsonResponse = EntityUtils.toString(response.getEntity());
                System.out.println("response");
                System.out.println(jsonResponse);
                return jsonResponse;
            } else {
                System.out.println("Error: Failed to retrieve data. Status Code: "
                        + response.getStatusLine().getStatusCode());
                return null;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    static class PageViewsData {
        // 根据 JSON 结构定义字段
        // 示例字段，你需要根据实际返回的 JSON 格式来调整
        private String items;

        @Override
        public String toString() {
            return "PageViewsData{" +
                    "items='" + items + '\'' +
                    '}';
        }
    }
}
