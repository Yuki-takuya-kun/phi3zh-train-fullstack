//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.io.IOException;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;
//
//public class test {
//    public static String readFileAsString(String fileName) throws IOException {
//        return new String(Files.readAllBytes(Paths.get(fileName)));
//    }
//
//    public static void main(String[] args) {
//        Pattern pat = Pattern.compile("<.*?>");
//
//        try {
//            String content = readFileAsString("output/test.txt");
//            Matcher mat = pat.matcher(content);
//            System.out.println(content);
//            while (mat.find()){
//                System.out.println(mat.group());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) {
        String input = "<a<b>";

        // 更精确的非贪婪匹配
        Pattern pattern = Pattern.compile("<[^<]*>");
        Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            System.out.println("Matched: " + matcher.group());
        }
    }
}


