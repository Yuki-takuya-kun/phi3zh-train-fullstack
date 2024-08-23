package phi3zh.common.utils;

import java.util.HashSet;
import java.util.Set;

public class Distance {
    public static <T> double jaccard(Set<T> A, Set<T> B){
        Set<T> interserction = new HashSet<>(A);
        Set<T> union = new HashSet<>(B);

        interserction.containsAll(B);
        union.addAll(B);

        double jaccardIndex = (double) interserction.size() / union.size();
        return 1.0 - jaccardIndex;
    }
}
