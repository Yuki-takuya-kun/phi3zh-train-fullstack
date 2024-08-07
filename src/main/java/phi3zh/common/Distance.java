package phi3zh.common;

import java.util.HashSet;
import java.util.Set;

public class Distance {
    public static <T> double computeJaccardDistance(Set<T> setA, Set<T> setB){
        Set<T> interserction = new HashSet<>(setA);
        Set<T> union = new HashSet<>(setB);

        interserction.containsAll(setB);
        union.addAll(setB);

        double jaccardIndex = (double) interserction.size() / union.size();
        return jaccardIndex;
    }

    public static <T> boolean isJaccardDistanceLargerThan(Set<T> setA, Set<T> setB, double threshold){
        assert threshold < 1 && threshold >0;
        return computeJaccardDistance(setA, setB) >= threshold;
    }
}
