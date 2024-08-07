package phi3zh.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

public class Hasher {

    public static List<Integer> MurmurHashForK_32(String text, int K, int seed){
        Random random = new Random(seed);
        HashFunction hashFunction = Hashing.murmur3_32(seed);
        Long oriHashCode = (long) hashFunction.hashString(text, StandardCharsets.UTF_8).asInt();
        int module = Integer.MAX_VALUE;
        int[] bArray = IntStream.generate(()-> random.nextInt(module)).limit(K).toArray();
        List<Integer> hashCodes = Arrays.stream(bArray).mapToLong(b -> (oriHashCode + b) % module)
                .mapToInt(i -> (int) i).boxed().collect(Collectors.toList());
        return hashCodes;
    }
}
