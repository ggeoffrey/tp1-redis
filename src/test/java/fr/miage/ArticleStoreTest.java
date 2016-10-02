package fr.miage;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by geoffrey on 27/09/2016.
 */
public class ArticleStoreTest {


    /**
     * The global Jedis connection to some Redis server
     */
    private static Jedis conn;

    /**
     * The global fr.miage.ArticleStore we are going to work with
     */
    private static ArticleStore articleStore;

    /**
     * A function stating if the first arg is superior or equal to the second one (formerly known as `≤` )
     * λ2,1.true
     * λ1,1.true
     * λ1,2.false
     */
    private static Function2<Long, Long, Boolean> inferiorOrEqual
            = (Long a, Long b) -> a.compareTo(b) >= 0;


    /**
     * Initialise the connection and the article store
     */
    @BeforeClass
    public static void setup(){
        conn = new Jedis("localhost", 6379);
        articleStore = new ArticleStore();
    }

    /**
     * Close the connection
     */
    @AfterClass
    public static void tearDown(){
        conn.close();
    }


    @Test
    public void addArticle() throws Exception {
        long id = articleStore.addArticle(conn, "user:0","My first article", "http://foo.bar/article");

        final String articleKey = ArticleStore.articleKey(id);
        assertTrue(conn.hget(articleKey, "title").equals("My first article"));
        assertTrue(conn.sismember(ArticleStore.votersKey(id),"user:0"));
        assertTrue(conn.hget(articleKey, "link").equals("http://foo.bar/article"));
        assertTrue(conn.hget(articleKey,"nbVotes").equals("1"));
    }

    @Test
    public void vote() throws Exception {
        articleStore.setExpireTime(TimeRange.SECOND);
        long id = articleStore.addArticle(conn, "user:1", "My second article", "some link...");

        assertTrue(articleStore.vote(conn, id, "user:1") == VoteStatus.ALREADY_VOTED);
        assertTrue(articleStore.vote(conn, id, "user:2") == VoteStatus.VOTED);
        Thread.sleep(1200);
        assertTrue(articleStore.vote(conn, id, "user:3") == VoteStatus.CANNOT_VOTE_ANYMORE);
    }


    @Test
    public void everyLong(){

        ArrayList<Long> evenColl = new ArrayList<Long>(){{
            add(4L);
            add(3L);
            add(2L);
            add(1L);
        }};
        ArrayList<Long> oddColl = new ArrayList<Long>(){{
            add(5L);
            add(4L);
            add(3L);
            add(2L);
            add(1L);
        }};
        ArrayList<Long> badColl = new ArrayList<Long>(){{
            add(3L);
            add(4L);
            add(2L);
            add(1L);
        }};
        assertTrue(ArticleStoreTestCompanion.everyLong(inferiorOrEqual, evenColl));
        assertTrue(ArticleStoreTestCompanion.everyLong(inferiorOrEqual, oddColl));
        assertFalse(ArticleStoreTestCompanion.everyLong(inferiorOrEqual, badColl));
    }


    @Test
    public void getNLatests() throws Exception {
        List<Map<String,String>> list = articleStore.getNLatests(conn, 100); // get the 100 latest articles

        ArrayList<Long> timestampList = new ArrayList<>();

        list.stream()                          // ∀
                .map(m->m.get("timestamp"))    // get the timestamp
                .map(Long::parseLong)          // parse it as a Long
                .forEach(timestampList::add);  // collect the results in timestampList

        assertTrue(ArticleStoreTestCompanion.everyLong(inferiorOrEqual, timestampList));
    }

    @Test
    public void getNMostUpvoted() throws Exception {
        List<Map<String,String>> list = articleStore.getNMostUpvoted(conn, 100); // get the 100 most upvoted articles

        ArrayList<Long> scoresList = new ArrayList<>();

        list.stream()                       // ∀
                .map(m->m.get("score"))     // get the timestamp
                .map(Long::parseLong)       // parse it as a Long
                .forEach(scoresList::add);  // collect the results in timestampList

        assertTrue(ArticleStoreTestCompanion.everyLong(inferiorOrEqual, scoresList));
    }

    @Test
    public void addCategory() throws Exception {
        long id = articleStore.addArticle(conn, "user:1","My third article", "http://foo.bar/article3");
        articleStore.addInCategory(conn, "java", id);

        String categoryKey = ArticleStore.categoryKey("java");
        String articleKey = ArticleStore.articleKey(id);

        assertTrue(conn.sismember(categoryKey, articleKey));
    }


    @Test
    public void getAllByCategory() throws Exception{
        List<Map<String, String>> articles = articleStore.getAllByCategory(conn, "java");
    }
}