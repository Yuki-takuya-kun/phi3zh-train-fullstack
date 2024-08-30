package phi3zh.dataconverter.deduplicator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Combinations;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import phi3zh.common.utils.Distance;
import phi3zh.common.utils.Hasher;
import phi3zh.config.LSHDeduplicatorConfig;
import phi3zh.dataconverter.SparkConverter;
import phi3zh.dataconverter.blocker.BlockerFactory;
import phi3zh.dataconverter.blocker.BlockerType;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.reflect.ClassTag$;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.spark.sql.functions.*;


public class LSHDeduplicator extends SparkConverter {

    private String dataDir; // the path of the inputTextDir
    private String outputDir; // the path of the outputTextDir
    private int p; // the number or combination of hash function
    private double t; // the threshold of Jaccard similarity
    private int k; // the word number in bag of words
    private int b; // the number of bonds
    private int r; // the row number in each bond
    private int seed;
    private SparkSession spark;
    private transient SparkContext sc;

    // sql and table name
    private static final String FILE_PATH = "FilePath";
    private static final String FILE_NAME = "FileName";
    private static final String TEXT = "Text";
    private static final String BLOCKID = "BlockId";
    private static final String BLOCKIDS = "BlockIds";
    private static final String BLOCKID1 = "BlockId1";
    private static final String BLOCKID2 = "BlockId2";
    private static final String BLOCKSEQ = "BlockSeq";
    private static final String NGRAMS = "Ngrams";
    private static final String HASH_LST = "HashList";
    private static final String BANDID = "BandId";
    private static final String NGRAMS1 = "ngrams1";
    private static final String NGRAMS2 = "ngrams2";
    private static final String VERTICEID = "verticeId";
    private static final String VERTICEID1 = "verticeId1";
    private static final String VERTICEID2 = "verticeId2";
    private static final String COMPONENT = "component";

    private static final String BLOCKS_TAB = "blocks";
    private static final String NGRAMS_TAB = "ngrams";
    private static final String CANDIDATE_TAB = "candidates";
    private static final String BLOCK2NGRAMS1_TAB = "block2Ngrams1";
    private static final String BLOCK2NGRAMS2_TAB = "Block2Ngrams2";
    private static final String SAMEBLOCK_TAB = "sameBlock";
    private static final String VERTICES_TAB = "vertices";
    private static final String DEDUPLICATED_VERTICE_TAB = "deduplicatedVertices";

    public LSHDeduplicator(LSHDeduplicatorConfig config){
        super(config.getSparkAppName(), config.getSparkMaster());
        this.dataDir = config.getDataDir();
        this.outputDir = config.getOutputDir();
        this.p = config.getP();
        this.t = config.getT();
        this.k = config.getK();
        this.b = config.getB();
        this.r = config.getR();
        this.seed = config.getSeed();
        this.spark = SparkSession.builder().appName("LSHDeduplicator")
                .config("spark.master", "local")
                .getOrCreate();
        this.sc = this.spark.sparkContext();
    }

    @Override
    protected Dataset<Row> process(Dataset<Row> files){
        try {
            Dataset<Row> blocksAndNgrams = split2BlocksAndNgrams(files);
            Dataset<Row> blocks = blocksAndNgrams.select(BLOCKID, TEXT);
            // persist blockDf to accelerate
            blocks.persist(StorageLevel.MEMORY_AND_DISK());
            blocks.createTempView(BLOCKS_TAB);
            Dataset<Row> ngrams = blocksAndNgrams.select(BLOCKID, NGRAMS);
            ngrams.createTempView(NGRAMS_TAB);
            Dataset<Row> hashes = ngrams2HashLst(ngrams);
            Dataset<Row> bands = split2Bands(hashes);
            Dataset<Row> sameBlocks = getSameBlocks(bands);
            sameBlocks.createTempView(SAMEBLOCK_TAB);
            Dataset<Row> deduplicatedBlocks = deduplicate(blocks);
            Dataset<Row> outputData = assembleBlocks(deduplicatedBlocks);
            return outputData;
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Dataset<Row> load(){
//        File folder = new File(this.dataDir);
//        File[] files = folder.listFiles();
//        if (files == null){
//            System.out.println("there are no files in the given dir");
//        }
//        List<Row> filePathList = Arrays.stream(folder.listFiles()).map(file -> RowFactory.create(file.getPath().toString())).collect(Collectors.toList());
//        Dataset<Row> filePaths = spark.createDataFrame(filePathList,
//                new StructType(new StructField[]{
//                        new StructField(FILE_PATH, DataTypes.StringType, false, Metadata.empty())
//                }));
//        Dataset<Row> fileDataset = filePaths.map(
//                (MapFunction<Row, Row>) row -> {
//                    Path filePath = Paths.get((String) row.getAs(FILE_PATH));
//                    String fileName = filePath.getFileName().toString();
//                    String fileText = new String(Files.readAllBytes(filePath));
//                    return RowFactory.create(fileName, fileText);
//                },
//                Encoders.row(new StructType(new StructField[]{
//                        new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty()),
//                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
//                }))
//        );
        //the following code only support for hadoop
        Dataset<Row> binaryFiles = spark.read().format("binaryFile").load(dataDir+"/*");
        spark.udf().register("binaryToString", (byte[] binaryData)->{
            if (binaryData == null){return"";}
            return new String(binaryData);
        }, DataTypes.StringType);
        Dataset<Row> textFiles = binaryFiles.withColumn("content", functions.callUDF("binaryToString",
                binaryFiles.col("content")));
        Integer pathIdx = textFiles.schema().fieldIndex("path");
        Integer contentIdx = textFiles.schema().fieldIndex("content");
        Dataset<Row> fileDataset = textFiles.map(
                (MapFunction<Row, Row>) row -> {
                    String path = row.getAs(pathIdx);
                    int fileIdx = path.lastIndexOf('/');
                    String fileName = path.substring(fileIdx+1);
                    String text = row.getAs(contentIdx);
                    return RowFactory.create(fileName, text);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        return fileDataset;
    }

    // save the file to file system
    @Override
    protected void save(Dataset<Row> dataset){
        Integer FILE_NAME_IDX = dataset.schema().fieldIndex(FILE_NAME);
        Integer TEXT_IDX = dataset.schema().fieldIndex(TEXT);
        Broadcast<Integer> broadcastFileName = sc.broadcast(FILE_NAME_IDX, ClassTag$.MODULE$.apply(Integer.class));
        Broadcast<Integer> broadcastText = sc.broadcast(TEXT_IDX, ClassTag$.MODULE$.apply(Integer.class));

        dataset.foreach(row -> {
            String fileName = row.getAs(broadcastFileName.value());
            String text = row.getAs(broadcastText.value());
            Files.write(Paths.get(this.outputDir, fileName), text.getBytes(StandardCharsets.UTF_8));
        });
    }

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

    private Dataset<Row> split2BlocksAndNgrams(Dataset<Row> df){
        final int FILE_NAME_IDX = df.schema().fieldIndex(FILE_NAME);
        final int TEXT_IDX = df.schema().fieldIndex(TEXT);
        Broadcast<Integer> broadcastFileName = sc.broadcast(FILE_NAME_IDX, ClassTag$.MODULE$.apply(Integer.class));
        Broadcast<Integer> broadcastText = sc.broadcast(TEXT_IDX, ClassTag$.MODULE$.apply(Integer.class));

        Dataset<Row> blockDf = df.flatMap(
                (FlatMapFunction<Row, Row>) row ->{
                    String fileName = row.getAs(broadcastFileName.value());
                    String text = row.getAs(broadcastText.value());
                    List<String> blocks = BlockerFactory.get(BlockerType.CHAPTER_BLOCKER).block(text);
                    List<Row> blockRows = IntStream.range(0, blocks.size()).mapToObj(i->{
                        String blockId = fileName + '_' + i;
                        String block = blocks.get(i);
                        List<String> ngrams = split2Ngrams(block, this.k);
                        Seq<String> ngramsSeq = JavaConverters.asScalaIteratorConverter(ngrams.iterator()).asScala().toSeq();
                        return RowFactory.create(blockId, block, ngramsSeq);
                    }).collect(Collectors.toList());
                    return blockRows.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(BLOCKID, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(NGRAMS, DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
                }))
        );
        return blockDf;
    }

    /**
     * transfer ngrams to hash codes
     * @param df
     * @return
     */
    private Dataset<Row> ngrams2HashLst(Dataset<Row> df){
        Dataset<Row> blcokId2Hash = df.map(
                (MapFunction<Row, Row>) row -> {
                    String blockId = row.getAs(BLOCKID);
                    Seq<String> blockNgramsSeq = row.getAs(NGRAMS);
                    List<String> blockNgramsLst = JavaConverters.seqAsJavaListConverter(blockNgramsSeq).asJava();
                    List<List<Integer>> hashMat = blockNgramsLst.stream().map(elem -> Hasher.MurmurHashForK_32(elem, this.p, this.seed))
                            .collect(Collectors.toList());
                    List<Long> hashList = IntStream.range(0, this.p).mapToLong(
                            col -> hashMat.stream().mapToLong(lst -> lst.get(col)).min().getAsLong()
                    ).boxed().collect(Collectors.toList());
                    Seq<Long> hashSeq = JavaConverters.asScalaIteratorConverter(hashList.iterator()).asScala().toSeq();
                    return RowFactory.create(blockId, hashSeq);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(BLOCKID, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(HASH_LST, DataTypes.createArrayType(DataTypes.LongType), false, Metadata.empty())
                }))
        );
        return blcokId2Hash;
    }

    // for each block, we calcualte it corresponding hash codes and split it to bands
    private Dataset<Row> split2Bands(Dataset<Row> hash){
        Dataset<Row> res = hash.flatMap(
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
                        new StructField(BLOCKID, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(BANDID, DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField(HASH_LST, DataTypes.createArrayType(DataTypes.LongType), false, Metadata.empty())
                }))
        );
        return res;
    }

    // get same blocks with id
    private Dataset<Row> getSameBlocks(Dataset<Row> bands) throws Exception{
        // get same block candidates according to bands
        Dataset<Row> groupedBands = bands.groupBy(BANDID, HASH_LST).agg(collect_set(BLOCKID).alias(BLOCKIDS));
        Dataset<Row> candidates = groupedBands.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    List<String> blockIDs = JavaConverters.seqAsJavaListConverter((Seq<String>)row.getAs(BLOCKIDS)).asJava();
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
                        new StructField(BLOCKID1, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(BLOCKID2, DataTypes.StringType, false, Metadata.empty())
                }))
        ).distinct();
        candidates.createTempView(CANDIDATE_TAB);

        // 0: candidates_table, 1: ngrams table, 2: BlockId1, 3:BlockId2, 4:ngrams, 5:BlockId
        String block2Ngrams1Format = "SELECT {0}.{2} AS {2}, {0}.{3} AS {3}, {1}.{4} AS {4} " +
                "FROM {0} INNER JOIN {1} " +
                "ON {0}.{2} = {1}.{5} ";
        String block2Ngrams1SQL = MessageFormat.format(block2Ngrams1Format, CANDIDATE_TAB, NGRAMS_TAB,
                BLOCKID1, BLOCKID2, NGRAMS, BLOCKID);
        Dataset<Row> Block2Ngrams1 = spark.sql(block2Ngrams1SQL);
        Block2Ngrams1.createTempView(BLOCK2NGRAMS1_TAB);

        // 0: candidates_table, 1: ngrams table, 2: BlockId1, 3:BlockId2, 4:ngrams, 5:BlockId
        String block2Ngrams2Format = "SELECT {0}.{2} AS {2}, {0}.{3} AS {3}, {1}.{4} AS {4} " +
                "FROM {0} INNER JOIN {1} " +
                "ON {0}.{3} = {1}.{5} ";
        String block2Ngrams2SQL = MessageFormat.format(block2Ngrams2Format, CANDIDATE_TAB, NGRAMS_TAB,
                BLOCKID1, BLOCKID2, NGRAMS, BLOCKID);
        Dataset<Row> Block2Ngrams2 = spark.sql(block2Ngrams2SQL);
        Block2Ngrams2.createTempView(BLOCK2NGRAMS2_TAB);

        // 0: Block2Ngrams1, 1: Block2Ngrams2, 2: BlockId1, 3:BlockId2, 4:ngrams, 5:ngrams1, 6: ngrams2
        String candidatesWithNgramsFormat = "SELECT {0}.{2} AS {2}, {0}.{3} AS {3}, {0}.{4} AS {5}, {1}.{4} AS {6} " +
                "FROM {0} INNER JOIN {1} " +
                "ON {0}.{2} = {1}.{2} AND {0}.{3} = {1}.{3} ";
        String candidatesWithNgramsSQL = MessageFormat.format(candidatesWithNgramsFormat, BLOCK2NGRAMS1_TAB, BLOCK2NGRAMS2_TAB,
                BLOCKID1, BLOCKID2, NGRAMS, NGRAMS1, NGRAMS2);
        Dataset<Row> candidatesWithNgrams = spark.sql(candidatesWithNgramsSQL);

        Dataset<Row> sameBlocks = candidatesWithNgrams.flatMap(
                (FlatMapFunction<Row, Row>) row -> {
                    String BlockId1 = row.getAs(BLOCKID1);
                    String BlockId2 = row.getAs(BLOCKID2);
                    Seq<String> ngrams1Seq = row.getAs(NGRAMS1);
                    Seq<String> ngrams2Seq = row.getAs(NGRAMS2);
                    List<String> ngrams1Lst = JavaConverters.seqAsJavaListConverter(ngrams1Seq).asJava();
                    List<String> ngrams2Lst = JavaConverters.seqAsJavaListConverter(ngrams2Seq).asJava();
                    Set<String> ngrams1Set = new HashSet<>(ngrams1Lst);
                    Set<String> ngrams2Set = new HashSet<>(ngrams2Lst);
                    List<Row> res = new ArrayList<>();
                    if (Distance.jaccard(ngrams1Set, ngrams2Set) <= this.t){
                        res.add(RowFactory.create(BlockId1, BlockId2));
                    }
                    return res.iterator();
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(BLOCKID1, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(BLOCKID2, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        return sameBlocks;
    }

    protected Dataset<Row> deduplicate(Dataset<Row> blocks) throws Exception{
        Dataset<Row> vertices = blocks.select(BLOCKID).withColumn(VERTICEID, monotonically_increasing_id());
        JavaRDD<Tuple2<Object, String>> verticesRDD = vertices.toJavaRDD().map(row -> new Tuple2<>(row.getLong(1), row.getString(0)));
        vertices.createTempView(VERTICES_TAB);
        // 0: sameBlocks, 1: vertices, 2:BlockID,  3: BlockID1, 4:BlockID2, 5:VerticeID, 6:VerticeID1, 7.VerticeID2
        String sameBlocksWithVerticeIDFormat = "SELECT {0}.{3} AS {3}, {0}.{4} AS {4}, a1.{5} AS {6} , a2.{5} AS {7} " +
                "FROM {0} " +
                "INNER JOIN {1} AS a1 ON a1.{2} = {0}.{3} " +
                "INNER JOIN {1} AS a2 ON a2.{2} = {0}.{4} ";
        String sameBlocksWithVerticeIDSQL = MessageFormat.format(sameBlocksWithVerticeIDFormat, SAMEBLOCK_TAB,
                VERTICES_TAB, BLOCKID, BLOCKID1, BLOCKID2, VERTICEID, VERTICEID1, VERTICEID2);
        Dataset<Row> sameBlocksWithVerticeId = spark.sql(sameBlocksWithVerticeIDSQL);
        JavaRDD<Edge<String>> sameBlocksEdges = sameBlocksWithVerticeId.javaRDD().map(row -> {
            long verticeId1 = row.getAs(VERTICEID1);
            long verticeId2 = row.getAs(VERTICEID2);
            return new Edge<>(verticeId1, verticeId2, "");
        }).distinct();

        // calculating the conntected components
        Graph graph = Graph.apply(verticesRDD.rdd(), sameBlocksEdges.rdd(), "", StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
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
                        new StructField(VERTICEID, DataTypes.LongType, false, Metadata.empty()),
                        new StructField(COMPONENT, DataTypes.LongType, false, Metadata.empty())
                })
        );

        Dataset<Row> deduplicatedVerticeID = connnectedID.dropDuplicates(COMPONENT);
        deduplicatedVerticeID.createTempView(DEDUPLICATED_VERTICE_TAB);
        // 0: Deduplicated_vertice_table, 1: Blocks with vertice id table, 2: blocks table,  3: VerticeID, 4:blockID, 5: Text
        String deduplicatedBlocksFormat = "SELECT {2}.{4} AS {4}, {2}.{5} AS {5} " +
                "FROM {1} " +
                "INNER JOIN {0} ON {0}.{3} = {1}.{3} " +
                "INNER JOIN {2} ON {1}.{4} = {2}.{4} ";
        String deduplicatedBlocksSQL = MessageFormat.format(deduplicatedBlocksFormat, DEDUPLICATED_VERTICE_TAB,
                VERTICES_TAB, BLOCKS_TAB, VERTICEID, BLOCKID, TEXT);
        Dataset<Row> deduplicatedBlocks = spark.sql(deduplicatedBlocksSQL);
        return deduplicatedBlocks;
    }

    // assemble the blosk according the remained blocks
    private Dataset<Row> assembleBlocks(Dataset<Row> blocks){
        Dataset<Row> fileNameAndIDAndText = blocks.map(
                (MapFunction<Row, Row>) row -> {
                    String blockId = row.getAs(BLOCKID);
                    String text = row.getAs(TEXT);
                    int seperatorIdx = blockId.lastIndexOf('_');
                    String fileName = blockId.substring(0, seperatorIdx);
                    int seq = Integer.parseInt(blockId.substring(seperatorIdx+1));
                    return RowFactory.create(fileName, seq, text);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(BLOCKSEQ, DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        Dataset<Row> assembleFilesUnorder = fileNameAndIDAndText.groupBy(FILE_NAME)
                .agg(collect_list(BLOCKSEQ).alias(BLOCKSEQ), collect_list(TEXT).alias(TEXT));
        Dataset<Row> assembledFiles = assembleFilesUnorder.map(
                (MapFunction<Row, Row>) row -> {
                    String fileName = row.getAs(FILE_NAME);
                    List<Integer> blockSeqs = JavaConverters.seqAsJavaListConverter((Seq<Integer>) row.getAs(BLOCKSEQ)).asJava();
                    List<String> blockTexts = JavaConverters.seqAsJavaListConverter((Seq<String>) row.getAs(TEXT)).asJava();
                    List<Pair<Integer, String>> blocksInFile = IntStream.range(0, blockSeqs.size())
                            .mapToObj(i -> Pair.of(blockSeqs.get(i), blockTexts.get(i))).collect(Collectors.toList());
                    blocksInFile.sort(Comparator.comparingInt(Pair::getLeft));
                    String output = String.join("\n", blocksInFile.stream().map(Pair::getRight).collect(Collectors.toList()));
                    return RowFactory.create(fileName, output);
                },
                Encoders.row(new StructType(new StructField[]{
                        new StructField(FILE_NAME, DataTypes.StringType, false, Metadata.empty()),
                        new StructField(TEXT, DataTypes.StringType, false, Metadata.empty())
                }))
        );
        return assembledFiles;
    }



}
