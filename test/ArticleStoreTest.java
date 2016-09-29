import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
/**
 * Created by geoffrey on 27/09/2016.
 */
public class ArticleStoreTest {


    private static Jedis conn;

    @BeforeClass
    public static void setup(){
        conn = new Jedis("localhost", 6379);
    }

    @AfterClass
    public static void tearDown(){
        conn.close();
    }

    @Test
    public void addArticle() throws Exception {
        ArticleStore as = new ArticleStore();
        long id = as.addArticle(conn, "user:0","My first article", "http://foo.bar/article");

        final String articleKey = ArticleStore.articleKey(id);
        assertTrue(conn.hget(articleKey, "title").equals("My first article"));
        assertTrue(conn.sismember(ArticleStore.votersKey(id),"user:0"));
        assertTrue(conn.hget(articleKey, "link").equals("http://foo.bar/article"));
        assertTrue(conn.hget(articleKey,"nbVotes").equals("1"));
    }

    @Test
    public void vote() throws Exception {
        ArticleStore as = new ArticleStore();
        as.setExpireTime(TimeRange.SECOND);
        long id = as.addArticle(conn, "user:1", "My second article", "some link...");

        assertTrue(as.vote(conn, id, "user:1") == VoteStatus.ALREADY_VOTED);
        assertTrue(as.vote(conn, id, "user:2") == VoteStatus.VOTED);
        Thread.sleep(1200);
        assertTrue(as.vote(conn, id, "user:3") == VoteStatus.CANNOT_VOTE_ANYMORE);
    }


    @Test
    public void getNLatests() throws Exception {
        ArticleStore as = new ArticleStore();
        List<Map<String,String>> list = as.getNLatests(conn, 100); // get the 100 latest articles

        ArrayList<Long> timestampList = new ArrayList<>();

        list.stream()                          // ∀
                .map(m->m.get("timestamp"))    // get the timestamp
                .map(Long::parseLong)          // parse it as a Long
                .forEach(timestampList::add);  // collect the results in timestampList


        // A λ stating if the first arg is superior or equal to the second one (formerly known as `≥` )
        // λ2,1.true
        // λ1,1.true
        // λ1,2.false
        Function2<Long, Long, Boolean> superiorOrEqual = (Long a, Long b) -> a.compareTo(b) >= 0;

        assertTrue(ArticleStoreTestCompanion.everyLong(superiorOrEqual, timestampList));
    }
    @Test

    public void getNMostUpvoted() throws Exception {
        ArticleStore as = new ArticleStore();
        List<Map<String,String>> list = as.getNMostUpvoted(conn, 100); // get the 100 most upvoted articles

        ArrayList<Long> scoresList = new ArrayList<>();

        list.stream()                       // ∀
                .map(m->m.get("score"))     // get the timestamp
                .map(Long::parseLong)       // parse it as a Long
                .forEach(scoresList::add);  // collect the results in timestampList




        // A λ stating if the first arg is superior or equal to the second one (formerly known as `≥` )
        // λ2,1.true
        // λ1,1.true
        // λ1,2.false
        Function2<Long, Long, Boolean> superiorOrEqual = (Long a, Long b) -> a.compareTo(b) >= 0;

        assertTrue(ArticleStoreTestCompanion.everyLong(superiorOrEqual, scoresList));
    }

    @Test
    public void addCategory() throws Exception {
        ArticleStore as = new ArticleStore();
        long id = as.addArticle(conn, "user:1","My third article", "http://foo.bar/article3");
        as.addInCategory(conn, "java", id);

        String categoryKey = ArticleStore.categoryKey("java");
        String articleKey = ArticleStore.articleKey(id);

        assertTrue(conn.sismember(categoryKey, articleKey));
    }



}