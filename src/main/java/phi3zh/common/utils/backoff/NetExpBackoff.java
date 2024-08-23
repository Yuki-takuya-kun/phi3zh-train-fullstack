package phi3zh.common.utils.backoff;

public class NetExpBackoff extends ExpBackoff {
    public NetExpBackoff(){
        super(new String[]{"java.net", "javax.net"});
    }
}
