package phi3zh.dataconverter.cleaner;

import com.github.houbb.opencc4j.util.ZhConverterUtil;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import phi3zh.common.utils.Kafka;
import phi3zh.config.WikihtmlCleanerConfig;
import phi3zh.dataconverter.StreamConverter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class WikihtmlCleaner extends StreamConverter<List<Pair<String, String>>> {

    private static final List<String> tagDiscard = Stream.of(new String[]{
            "meta", "title", "img", "style", "annotation", "table"
    }).collect(Collectors.toList());

    private static final List<String> classAttrDiscard = Stream.of(new String[]{
            "ambox", "noteTA-group", "infobox"
    }).collect(Collectors.toList());

    private static final Stream<ImmutableTriple<String, String, String>> tagAttrDiscardStream = Stream.of(
            ImmutableTriple.of("table", "style", "border:1px solid #ddd; text-align:center; margin: auto;")
    );

    private static final Map<String, Map<String, List<String>>> tagAttrDiscard = tagAttrDiscardStream.collect(
            Collectors.groupingBy(
                    ImmutableTriple::getLeft,
                    Collectors.groupingBy(
                            ImmutableTriple::getMiddle,
                            Collectors.mapping(ImmutableTriple::getRight, Collectors.toList())
                    )
            )
    );

    // html tag
    private static final String BOLD = "b";
    private static final String ITALIC = "i";
    private static final String HEADER = "h";
    private static final String MULTI_CODE = "syntaxhighlight";
    private static final String SINGLE_CODE = "code";
    private static final List<String> NEWLINES = Stream.of(new String[]{
            "section", "p"
    }).collect(Collectors.toList());

    private static final Pattern CHAPTER_HTML_PATTERN = Pattern.compile("#(#+) .*");
    private static final Pattern TABLE_BEGIN_PATTERN = Pattern.compile("ul|ol");
    private static final String NEWLINETAG = "[newLine]";

    private static final String ORDER = "order";
    private static final String UNORDER = "unorder";
    private static final String NOTLIST = "notList";

    String bootstrapServers;
    String sourceTopic;
    String groupId;
    String outputDir;
    String endBucketName;
    String resourceSemaphoreName;
    int pollNum;
    Config redisConfig;
    Consumer consumer;
    RedissonClient redissonClient;

    public WikihtmlCleaner(WikihtmlCleanerConfig config){
        super(config.redisConfig(), config.endBucketName());
        this.bootstrapServers = config.kafkaServer();
        this.sourceTopic = config.sourceTopic();
        this.groupId = config.groupId();
        this.pollNum = config.pollNum();
        this.outputDir = config.outputDir();
        this.redisConfig = config.redisConfig();
        this.endBucketName = config.endBucketName();
        this.resourceSemaphoreName = config.resouceSemaphoreName();

        consumer = Kafka.getStringConsumer(bootstrapServers, groupId, Collections.singletonList(sourceTopic), pollNum);
        redissonClient = Redisson.create(this.redisConfig);
    }

    @Override
    protected List<Pair<String, String>> load(){
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        int recordCount = records.count();
        redissonClient.getSemaphore(resourceSemaphoreName).release(recordCount);
        List<Pair<String, String>> result = StreamSupport.stream(records.spliterator(), false)
                .map(record->Pair.of(record.key(), record.value())).collect(Collectors.toList());
        return result;
    }

    @Override
    protected List<Pair<String, String>> process(List<Pair<String, String>> data){
        return data.stream().map(elem -> {
            String title = elem.getLeft();
            String page = elem.getRight();
            page = clean(page);
            page = ZhConverterUtil.toSimple(page);
            return Pair.of(title, page);
        }).collect(Collectors.toList());
    }

    @Override
    protected void save(List<Pair<String, String>> data){
        data.stream().forEach(elem->{
            try {
                Files.write(Paths.get(outputDir, titleToFileName(elem.getLeft())), elem.getRight().getBytes(StandardCharsets.UTF_8));
            } catch (Exception e){
                e.printStackTrace();
            }
        });
    }

    @Override
    protected void shutdown(){
        consumer.close();
        redissonClient.shutdown();
    }

    /**
     * clean the wiki html text into the markdown format.
     * @param page the html page of the wiki
     * @return the markdown format that has been cleaned
     */
    protected String clean(String page){
        org.jsoup.nodes.Document wikiDoc = Jsoup.parse(page);
        Element root = wikiDoc.root();
        Element node = root;
        boolean forward = true; // denotes it is a forward or backward process
        while (root.childrenSize() > 0) {
            if (forward && !shouldDeleteTag(node)) {
                if (this.TABLE_BEGIN_PATTERN.matcher(node.tagName()).matches()) {
                    node.text(parseList(node));
                    if (node.nextElementSibling() != null) {
                        node = node.nextElementSibling();
                    } else {
                        node = node.parent();
                        forward = false;
                    }
                } else if (node.childrenSize() > 0) {
                    node = node.child(0);
                } else {
                    node.text(transferHtmlToMarkdown(node.tagName(), node.text()));
                    if (node.nextElementSibling() != null) {
                        node = node.nextElementSibling();
                    } else {
                        node = node.parent();
                        forward = false;
                    }
                }
            } else if (!forward) {
                node.html(transferHtmlToMarkdown(node.tagName(), node.text()));
                if (node.nextElementSibling() != null) {
                    forward = true;
                    node = node.nextElementSibling();
                } else {
                    node = node.parent();
                }
            } else {
                Element tmp;
                if (node.nextElementSibling() != null) {
                    tmp = node.nextElementSibling();
                } else {
                    tmp = node.parent();
                    forward = false;
                }
                node.remove();
                node = tmp;
            }
        }

        String res = root.text().replaceAll(
                this.NEWLINETAG.replace("[", "\\[").replace("]", "\\]"),
                "\n").replace("[编辑]", "");

        page = res;
        res = "";
        Matcher chapterMat = this.CHAPTER_HTML_PATTERN.matcher(page);
        int chapterStart = 0;
        int chapterEnd = 0;
        while (chapterMat.find()){
            if (page.substring(chapterEnd, chapterMat.start()).trim().length() > 0){
                res = res + page.substring(chapterStart, chapterMat.start());
            }
            chapterStart = chapterMat.start();
            chapterEnd = chapterMat.end();
        }
        if (page.substring(chapterEnd).trim().length() > 0){
            res = res + page.substring(chapterStart);
        }

        // delete the null lines
        String tmp = res;
        res = "";
        boolean isStart = true;
        int spaceLineCount = 0;
        for (String line: tmp.split("\n")){
            if (line.trim().length() > 0){
                isStart = false;
                spaceLineCount = 0;
                res += line + "\n";
            } else if (!isStart && spaceLineCount == 0){
                res += "\n";
                spaceLineCount ++;
            }
        }
        return res;
    }

    // assert the html tag should be delete or not
    private boolean shouldDeleteTag(Element node){
        // discard the tag
        if (this.tagDiscard.contains(node.tagName())){
            return true;
        }

        if (this.tagAttrDiscard.containsKey(node.tagName())){
            Map<String, List<String>> tmp = this.tagAttrDiscard.get(node.tagName());
            for (String attrName: tmp.keySet()){
                if (node.hasAttr(attrName)){
                    for (String attr: tmp.get(attrName)){
                        if (attr.equals(node.attr(attrName))){
                            return true;
                        }
                    }
                }
            }
        }

        for (String attr: this.classAttrDiscard){
            if (node.hasClass(attr)){
                return true;
            }
        }
        return false;
    }

    /**
     * @param content
     * @return
     */
    private String transferHtmlToMarkdown(String tagName, String content){
        if (tagName.equals(this.BOLD)){
            return "**" + content + "**";
        } else if (tagName.equals(this.ITALIC)){
            return "*" + content + "*";
        } else if (tagName.equals(this.MULTI_CODE)){
            return "```" + content + "```";
        } else if (tagName.equals(this.SINGLE_CODE)){
            return "`" + content + "`";
        }
        // if it is a header
        else if (tagName.length() == 2 && tagName.substring(0, 1).equals(this.HEADER) &&
                Character.isDigit(tagName.charAt(1))){
            int headerLevel = Integer.parseInt(tagName.substring(1,2));
            StringBuilder resBuilder = new StringBuilder();
            resBuilder.append(this.NEWLINETAG);
            resBuilder.append(this.NEWLINETAG);
            for (int i = 0; i < headerLevel; i++){
                resBuilder.append("#");
            }
            resBuilder.append(" ");
            resBuilder.append(content);
            return resBuilder.toString();
        } else if (this.NEWLINES.contains(tagName)){
            return this.NEWLINETAG + content;
        }
        else {
            return content;
        }
    }

    private String parseList(Element table){
        return parseList(table, 0) + this.NEWLINETAG;
    }

    private String parseList(Element table, int depth){
        int seq = 1;
        String listType = getListType(table.tagName());
        if (table.childrenSize() == 0){
            return "";
        }
        Element elem = table.child(0);
        StringBuilder resBuilder = new StringBuilder();
        while (elem !=null){
            if (elem.childrenSize() == 0 || !this.TABLE_BEGIN_PATTERN.matcher(elem.child(0).tagName()).matches()){
                StringBuilder tmpBuilder = new StringBuilder();
                tmpBuilder.append(this.NEWLINETAG);
                for (int i = 0; i < depth; i ++){
                    tmpBuilder.append("    ");
                }
                if (listType.equals(this.ORDER)){
                    tmpBuilder.append(String.format("%d. %s", seq, elem.text()));
                    seq += 1;
                } else if (listType.equals(this.UNORDER)){
                    tmpBuilder.append(String.format("- %s", elem.text()));
                }
                resBuilder.append(tmpBuilder);
            }
            else{
                Element child = elem.child(0);
                resBuilder.append(parseList(child, depth + 1));
            }
            elem = elem.nextElementSibling();
        }
        return resBuilder.toString();
    }

    private String getListType(String tagName){
        if (tagName.equals("ol")){
            return this.ORDER;
        } else if (tagName.equals("ul")){
            return this.UNORDER;
        } else {
            return this.NOTLIST;
        }
    }

    private String titleToFileName(String title){
        title = title.replaceAll("<|>|:|\"|/|\\|\\||\\?|\\*", "_");
        return title + ".txt";
    }

}
