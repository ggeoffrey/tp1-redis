package fr.miage;

import java.util.ArrayList;

/**
 * Provide higher order functions to work on collections.
 * Created by geoffrey on 27/09/2016.
 */
public class ArticleStoreTestCompanion {

    /**
     * State if the predicate is true fon every consecutive pairs from a list of Longs
     * @param predicate  A Long-»Long-»Boolean predicate
     * @param coll A collection of Longs
     * @return
     */
    public static boolean everyLong(Function2<Long, Long, Boolean> predicate, ArrayList<Long> coll){

        // We are going to perform some boolean ANDs, the neutral value for AND is 'true', as
        // is 0 for addition and 1 for multiplication.
        boolean result = true;

        // We cache the collection size, minus one because we work on pairs and we want to end
        // on the final pair as (n-1, n).
        final int size = coll.size() - 1;

        for (int i = 0; i < size; i++) {
            // We execute the predicate for the tuple (n, n+1).
            // performing a boolean AND between the result of the last pair and this one.
            result &= predicate.apply(coll.get(i), coll.get(i + 1));

            // If the predicate is false, it is pointless to continue.
            if (!result) break;
        }
        return result;
    }

}