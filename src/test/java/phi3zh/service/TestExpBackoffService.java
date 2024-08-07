package phi3zh.service;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.stereotype.Service;
import phi3zh.common.annotations.network.ExpBackoff;

import javax.net.ssl.SSLHandshakeException;

@Service
public class TestExpBackoffService {
    @ExpBackoff
    public void testMethod() throws Exception{

        throw new SSLHandshakeException("");
    }
}
