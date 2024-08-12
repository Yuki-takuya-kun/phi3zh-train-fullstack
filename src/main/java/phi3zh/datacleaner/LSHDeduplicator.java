package phi3zh.datacleaner;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Combinations;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import phi3zh.common.utils.Distance;
import phi3zh.common.utils.Hasher;
import phi3zh.datacleaner.blockers.ChapterBlocker;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.apache.spark.sql.functions.*;


public class LSHDeduplicator extends AbstractCleaner<Pair<String, String>>{

    String dataDir; // the path of the inputTextDir
    String outputDir; // the path of the outputTextDir
    int p; // the number or combination of hash function
    double t; // the threshold of Jaccard similarity
    int k; // the word number in bag of words
    int b; // the number of bonds
    int r; // the row number in each bond
    int seed;
    SparkSession spark;

    // pre compute pattern
    Pattern SPLIT_PATTERN = Pattern.compile("[\\u4e00-\\u9fa5]|[a-zA-Z]+|\\s+");
    Pattern ZH_CHAR_PATTERN = Pattern.compile("[\\u4e00-\\u9fa5]");
    Pattern EN_WORD_PATTERN = Pattern.compile("[a-zA-Z]+");
    Pattern SPACE_PATTERN = Pattern.compile("\\s+");

    Logger logger = LogManager.getLogger("DeduplicatorLogger");

    /**
     *
     * @param dataDir the path of the input texts
     * @param outputDir the path of output texts
     * @param p the number or combination of hash functions
     * @param t the threshold of Jaccard similarity
     * @param k the word number in bag of words
     * @param b the number of bonds
     * @param r the row number in each bond
     * @param seed the seed of random utilities
     */
    @Autowired
    public LSHDeduplicator(String dataDir, String outputDir, int p, double t, int k, int b, int r, int seed){
        this.dataDir = dataDir;
        this.outputDir = outputDir;
        this.p = p;
        this.t = t;
        this.k = k;
        this.b = b;
        this.r = r;
        assert b*r <= p;
        this.seed = seed;
        this.spark = SparkSession.builder().appName("LSHDeduplicator")
                .config("spark.master", "local")
                .config("spark.driver.memory", "8g")
                .config("spark.executor.memory", "8g")
                .getOrCreate();

    }

    @Override
    protected void produceElements() {
        File folder = new File(this.dataDir);
        File[] files = folder.listFiles();
        if (files == null){
            System.out.println("there are no files in the given dir");
            return;
        }
        List<Row> filePathList = Arrays.stream(folder.listFiles()).map(file -> RowFactory.create(file.getPath().toString())).collect(Collectors.toList());
        Dataset<Row> filePaths = spark.createDataFrame(filePathList,
                new StructType(new StructField[]{
                        new StructField("FilePath", DataTypes.StringType, false, Metadata.empty())
                }));

        // load all files and split it into blocks, the first column is file name with sequence id of the block in a
        // given file, the sencond is the ngrams that split with k and the third element is the source text of block.
        Dataset<Row> blockDf = filePaths.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    Path filePath = Paths.get((String) row.getAs("FilePath"));
                    String fileName = filePath.getFileName().toString();
                    String text = new String(Files.readAllBytes(filePath));
                    ChapterBlocker chapterBlocker = new ChapterBlocker();
                    List<String> blocks = chapterBlocker.block(text);
                    Iterator<Row> resultBlock = IntStream.range(0, blocks.size()).mapToObj(i -> {
                        String block = blocks.get(i);
                        List<String> bagOfWords = split2Ngrams(block, this.k);
                        Seq<String> bagOfWordsSeq = JavaConverters.asScalaIteratorConverter(bagOfWords.iterator()).asScala().toSeq();
                        String blockID = fileName + "_" + i;
                        return RowFactory.create(blockID, bagOfWordsSeq, block);
                    }).collect(Collectors.toList()).iterator();
                    return resultBlock;
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("BlockID", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Ngrams", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty()),
                        new StructField("BlockText", DataTypes.StringType, false, Metadata.empty())
                }))
        );

        // persist the blockDf to accelerate
        blockDf.persist(StorageLevel.MEMORY_AND_DISK());

        Dataset<Row> id2Hash = blockDf.map(
                (MapFunction<Row, Row>) row -> {
                    String blockID = row.getAs("BlockID");
                    List<String> ngrams = JavaConverters.seqAsJavaListConverter((Seq<String>)row.getAs("Ngrams")).asJava();
                    List<List<Integer>> hashMat = ngrams.stream()
                            .map(ngram -> Hasher.MurmurHashForK_32(ngram, this.p, this.seed))
                            .collect(Collectors.toList());
                    List<Long> hashList = IntStream.range(0, this.p).mapToLong(
                            col -> hashMat.stream().mapToLong(lst -> lst.get(col)).min().getAsLong()
                    ).boxed().collect(Collectors.toList());
                    Seq<Long> hashSeq = JavaConverters.asScalaIteratorConverter(hashList.iterator()).asScala().toSeq();
                    return RowFactory.create(blockID, hashSeq);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("BlockID", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Hash", DataTypes.createArrayType(DataTypes.LongType), false, Metadata.empty())
                }))
        );

        // for each block, we calcualte it corresponding hash codes and split it to bands
        Dataset<Row> id2Bands = id2Hash.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    String blockID = row.getString(0);
                    List<Long> blockHash = row.getList(1);
                    List<Row> result = IntStream.range(0, b)
                            .mapToObj(i -> RowFactory.create(blockID, i,
                                    JavaConverters.asScalaIteratorConverter(blockHash.subList(r*i, r*(i+1)).iterator())
                                            .asScala().toSeq()))
                            .collect(Collectors.toList());
                    return result.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("BlockID", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("BandID", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("Hash", DataTypes.createArrayType(DataTypes.LongType), false, Metadata.empty())
                }))
        );

        // group the id with same BandID and Hash
        Dataset<Row> groupBandHash = id2Bands.groupBy("BandID", "Hash").agg(collect_set("BlockID").alias("BlockIDs"));

        // extract similar candidates
        Dataset<Row> candidates = groupBandHash.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    List<String> blockIDs = JavaConverters.seqAsJavaListConverter((Seq<String>)row.getAs("BlockIDs")).asJava();
                    List<Row> localCandidates = new ArrayList<>();
                    if (blockIDs.size() <= 1){return localCandidates.iterator();}
                    Combinations combinations = new Combinations(blockIDs.size(), 2);
                    for (int[] combination: combinations){
                        String blockID1 = blockIDs.get(combination[0]);
                        String blockID2 = blockIDs.get(combination[1]);
                        localCandidates.add(RowFactory.create(blockID1, blockID2));
                    }
                    return localCandidates.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("BlockID1", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("BlockID2", DataTypes.StringType, false, Metadata.empty())
                }))
        ).distinct();

        Dataset<Row> id1Ngrams = candidates.join(blockDf.select("BlockID", "Ngrams"),
                candidates.col("BlockID1").equalTo(blockDf.col("BlockID")), "inner")
                .withColumnRenamed("Ngrams", "Ngrams1");
        Dataset<Row> id2Ngrams = candidates.join(blockDf.select("BlockID", "Ngrams"),
                candidates.col("BlockID2").equalTo(blockDf.col("BlockID")), "inner")
                .withColumnRenamed("Ngrams", "Ngrams2");
        Dataset<Row> ngramsCandidates = id1Ngrams.join(id2Ngrams,
                id1Ngrams.col("BlockID1").equalTo(id2Ngrams.col("BlockID1"))
                        .and(id1Ngrams.col("BlockID2").equalTo(id2Ngrams.col("BlockID2"))),
                "inner");
        // calculating same blocks
        Dataset<Row> sameBlocks = ngramsCandidates.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    String blockID1 = row.getAs("BlockID1");
                    String blockID2 = row.getAs("BlockID2");
                    Set<String> ngrams1 = new HashSet<>(JavaConverters.seqAsJavaListConverter((Seq<String>)row.getAs("Ngrams1")).asJava());
                    Set<String> ngrams2 = new HashSet<>(JavaConverters.seqAsJavaListConverter((Seq<String>)row.getAs("Ngrams2")).asJava());
                    List<Row> result = new ArrayList<>();
                    if (Distance.isJaccardDistanceLargerThan(ngrams1, ngrams2, this.t)){
                        result.add(RowFactory.create(blockID1, blockID2));
                    }
                    return result.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("BlockID1", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("BlockID2", DataTypes.StringType, false, Metadata.empty())
                }))
        );

        Dataset<Row> verticesDf = blockDf.select("BlockID").withColumn("ID", monotonically_increasing_id());
        this.logger.info(String.format("Before deduplication, there has %d blocks", verticesDf.count()));

        JavaRDD<Tuple2<Object, String>> verticesRDD = verticesDf.toJavaRDD().map(row -> new Tuple2<>(row.getLong(1), row.getString(0)));

        Dataset<Row> sameBlocksWithID = sameBlocks
                .join(verticesDf, sameBlocks.col("BlockID1").equalTo(verticesDf.col("BlockID")))
                .withColumnRenamed("ID", "ID1").select("ID1", "BlockID1", "BlockID2")
                .join(verticesDf, sameBlocks.col("BlockID2").equalTo(verticesDf.col("BlockID")))
                .withColumnRenamed("ID", "ID2").select("ID1", "ID2", "BlockID1", "BlockID2");
        // map to edges
        JavaRDD<Edge<String>> edges = sameBlocksWithID.javaRDD().map(
                row -> {
                    String blockID1 = row.getAs("BlockID1");
                    String blockID2 = row.getAs("BlockID2");
                    long id1 = row.getAs("ID1");
                    long id2 = row.getAs("ID2");
                    return new Edge<>(id1, id2, "");
                }
        ).distinct();

        // calculating the conntected components
        Graph graph = Graph.apply(verticesRDD.rdd(), edges.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(String.class), ClassTag$.MODULE$.apply(String.class));

        Graph connectedGraph = graph.ops().connectedComponents();

        Dataset<Row> connnectedID = spark.createDataFrame(
                connectedGraph.vertices().toJavaRDD().map(vertice -> {
                    Tuple2<Long, Long> v = (Tuple2<Long, Long>) vertice;
                    long id = v._1();
                    long component = v._2();
                    return RowFactory.create(id, component);
                }),
                new StructType(new StructField[]{
                        new StructField("ID", DataTypes.LongType, false, Metadata.empty()),
                        new StructField("Component", DataTypes.LongType, false, Metadata.empty())
                })
        );

        // delete blocks
        Dataset<Row> connectedDuplicated = connnectedID.dropDuplicates("Component");
        Dataset<Row> blockIDDuplicated = verticesDf.join(connectedDuplicated, "ID"); // with ID and BlockID

        this.logger.info(String.format("After deduplicate, there has %d blocks remain.", blockIDDuplicated.count()));
        Dataset<Row> blockDuplicated = blockIDDuplicated.join(blockDf.select("BlockID", "BlockText"), "BlockID")
                .select("BlockID", "BlockText");

        // split block with fileName and sequence
        Dataset<Row> splitBlockID = blockDuplicated.map(
                (MapFunction<Row, Row>) row -> {
                    String blockID = row.getAs("BlockID");
                    String text = row.getAs("BlockText");
                    int seperateIdx = blockID.lastIndexOf('_');
                    String fileName = blockID.substring(0, seperateIdx);
                    int sequence = Integer.parseInt(blockID.substring(seperateIdx+1));
                    return RowFactory.create(fileName, sequence, text);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField("FileName", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("Seq", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("BlockText", DataTypes.StringType, false, Metadata.empty())
                }))
        );

        Dataset<Row> groupWithFileName = splitBlockID.groupBy("FileName")
                .agg(collect_list("Seq").alias("Seqs"),
                        collect_list("BlockText").alias("Texts"));

        groupWithFileName.foreach(row -> {
            String fileName = row.getAs("FileName");
            List<Integer> sequences = JavaConverters
                    .seqAsJavaListConverter((Seq<Integer>) row.getAs("Seqs")).asJava();
            List<String> texts = JavaConverters
                    .seqAsJavaListConverter((Seq<String>) row.getAs("Texts")).asJava();
            List<Tuple2<Integer, String>> seqTexts = IntStream.range(0, sequences.size())
                    .mapToObj(i -> new Tuple2<>(sequences.get(i), texts.get(i))).collect(Collectors.toList());
            seqTexts.sort(Comparator.comparingInt(Tuple2::_1));
            String output = String.join("\n", seqTexts.stream().map(elem->elem._2()).collect(Collectors.toList()));
            Files.write(Paths.get(this.outputDir, fileName), output.getBytes(StandardCharsets.UTF_8));
        });

    }

    @Override
    protected void consumeElements() {}

    private List<String> split2Ngrams(String inputText, int k){
        List<String> ngrams = new ArrayList<>();
        List<String> grams = new ArrayList<>();
        String enChars = "";
        for (char c: inputText.toCharArray()){
            if (c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') enChars += c;
            else {
                if (enChars.length() > 0){
                    grams.add(enChars);
                    enChars = "";
                }
                if (Character.isWhitespace(c)) continue;
                else grams.add(String.valueOf(c));
            }
        }
        for (int i=0; i < grams.size()-k+1; i++){
            ngrams.add(String.join(" ", grams.subList(i, i+k)));
        }
        return ngrams;
    }

}
