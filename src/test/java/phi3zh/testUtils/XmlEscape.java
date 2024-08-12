package phi3zh.testUtils;

public class XmlEscape {
    public static String escape(String text){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++){
            char c = text.charAt(i);
            switch (c){
                case '<':
                    sb.append("&lt;");
                    break;
                case '>':
                    sb.append("&gt;");
                    break;
                case '"':
                    sb.append("&quot;");
                    break;
                case '\'':
                    sb.append("&apos;");
                    break;
                case '&':
                    sb.append("&amp;");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }
}
