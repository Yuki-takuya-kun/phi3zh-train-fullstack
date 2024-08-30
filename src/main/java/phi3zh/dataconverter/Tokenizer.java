package phi3zh.dataconverter;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import phi3zh.common.Tokenize;
import phi3zh.config.TokenizerConfig;
import phi3zh.dataconverter.blocker.ChapterBlocker;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Tokenizer extends SparkConverter {

    private static final String FILE_NAME = "fileName";
    private static final String BLOCKID = "blockId";
    private static final String TOKENS = "tokens";
    private static final String TEXT = "text";

    private String hostName;
    private int size;
    private int port;

    private String dataPath;
    private String outputFile;


    public Tokenizer(TokenizerConfig config){
        super(config.getSparkAppName(), config.getSparkMaster());
        this.size = config.size();
        this.hostName = config.getHostName();
        this.port = config.port();
        this.dataPath = config.getDataPath();
        this.outputFile = config.getOutputFile();
    }

    @Override
    protected Dataset<Row> load(){
        Dataset<Row> binaryFiles = sparkSession.read().format("binaryFile").load(dataPath+"/*");
        sparkSession.udf().register("binaryToString", (byte[] binaryData) -> {
            if (binaryData == null){
                return "";
            }
            return new String(binaryData);
        }, DataTypes.StringType);
        Dataset<Row> textFiles = binaryFiles.withColumn("content",
                functions.callUDF("binaryToString", binaryFiles.col("content")));
        Integer pathIdx = textFiles.schema().fieldIndex("path");
        Integer textIdx = textFiles.schema().fieldIndex("content");
        Dataset<Row> fileDataset = textFiles.map(
                (MapFunction<Row, Row>) row -> {
                    String path = row.getAs(pathIdx);
                    int separateIdx = path.lastIndexOf('/');
                    String fileName = path.substring(separateIdx+1);
                    String content = row.getAs(textIdx);
                    return RowFactory.create(fileName, content);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        return fileDataset;
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> data){
        int fileNameIdx = data.schema().fieldIndex(FILE_NAME);
        int contentIdx = data.schema().fieldIndex(TEXT);
        Dataset<Row> blocks = data.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    String fileName = row.getAs(fileNameIdx);
                    String content = row.getAs(contentIdx);
                    ChapterBlocker blocker = ChapterBlocker.getInstance();
                    List<String> contentBlocks = blocker.block(content);
                    List<Row> contentBlocksWithSeq = IntStream.range(0, contentBlocks.size()).mapToObj(i -> {
                        String fileId = fileName + '_' + i;
                        String contentBlock = contentBlocks.get(i);
                        return RowFactory.create(fileId, contentBlock);
                    }).collect(Collectors.toList());
                    return contentBlocksWithSeq.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(BLOCKID, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        int blockIdx = blocks.schema().fieldIndex(BLOCKID);
        int textIdx = blocks.schema().fieldIndex(TEXT);
        return blocks.mapPartitions(
                (MapPartitionsFunction<Row, Row>)iterator -> {
                    List<String> blockIdCache = new ArrayList<>();
                    List<String> contentCache = new ArrayList<>();
                    List<Row> results = new ArrayList<>();
                    while (iterator.hasNext()){
                        Row row = iterator.next();
                        blockIdCache.add(row.getAs(blockIdx));
                        contentCache.add(row.getAs(textIdx));
                        if (contentCache.size() >= this.size || !iterator.hasNext()){
                            List<List<Integer>> tokens = tokenize(contentCache);
                            IntStream.range(0, contentCache.size()).forEach(i -> {
                                Seq<Integer> tokensSeq = JavaConverters.asScala(tokens.get(i)).toSeq();
                                results.add(RowFactory.create(blockIdCache.get(i), tokensSeq));
                            });
                            blockIdCache.clear();
                            contentCache.clear();
                        }
                    }
                    return results.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(BLOCKID, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TOKENS, DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty())
                }))
        );
    }

    @Override
    protected void save(Dataset<Row> data){
        int blockIdx = data.schema().fieldIndex(BLOCKID);
        int tokenIdx = data.schema().fieldIndex(TOKENS);
        data.foreachPartition(iterator -> {
            try (PrintWriter pw = new PrintWriter(new FileWriter(outputFile, true))){
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    String blockId = row.getAs(blockIdx);
                    Seq<Integer> tokensSeq = row.getAs(tokenIdx);
                    List<Integer> tokenList = JavaConverters.seqAsJavaListConverter(tokensSeq).asJava();
                    JsonArray tokens = new JsonArray();
                    tokenList.stream().forEach(elem -> tokens.add(elem));
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty(BLOCKID, blockId);
                    jsonObject.add(TOKENS, tokens);
                    pw.println(jsonObject);
                }
            }
        });
    }

    private List<List<Integer>> tokenize(List<String> texts){
        try (Socket socket = new Socket(hostName, port)){
            Tokenize.TextList.newBuilder();
            Tokenize.TextList.Builder textListBuilder = Tokenize.TextList.newBuilder();
            texts.stream().forEach(text-> textListBuilder.addText(text));
            Tokenize.TextList textList = textListBuilder.build();
            OutputStream outputStream = socket.getOutputStream();
            int dataLength = textList.getSerializedSize();
            outputStream.write(ByteBuffer.allocate(4).putInt(dataLength).array());
            textList.writeTo(outputStream);
            outputStream.flush();

            List<List<Integer>> result = new ArrayList<>();
            try(InputStream inputStream = socket.getInputStream()){
                byte[] lengthBytes = new byte[4];
                inputStream.read(lengthBytes);
                int length = ByteBuffer.wrap(lengthBytes).getInt();
                byte[] data = new byte[length];
                int totalRead = 0;
                while (totalRead < length){
                    int bytesRead = inputStream.read(data, totalRead, length-totalRead);
                    if (bytesRead == -1){
                        throw new RuntimeException("do not read the data");
                    }
                    totalRead += bytesRead;
                }
                Tokenize.TokenList tokenList = Tokenize.TokenList.parseFrom(data);
                tokenList.getListList().stream().forEach(tokens -> result.add(tokens.getTokensList()));
            }
            return result;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
