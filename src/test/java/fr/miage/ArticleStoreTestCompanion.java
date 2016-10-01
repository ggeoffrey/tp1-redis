package fr.miage;

import fr.miage.Function2;

import java.util.ArrayList;

/**
 * Created by geoffrey on 27/09/2016.
 */
public class ArticleStoreTestCompanion {

    /**
     * State if the predicate is true fon every consecutive pairs from a list of Longs
     * @param predicate  A Long->Long->Boolean predicate
     * @param coll A collection of Longs
     * @return
     */
    public static boolean everyLong(Function2<Long, Long, Boolean> predicate, ArrayList<Long> coll){
        boolean result = true;
        final int size = coll.size() - 1;
        for (int i = 0; i < size; i++) {
            result &= predicate.apply(coll.get(i), coll.get(i + 1));
            if (!result) break;
        }
        return result;
    }

}