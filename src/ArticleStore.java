import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by geoffrey on 26/09/2016.
 */
public class ArticleStore {

    private static int expireTime;
    private static final int voteIncrement = 457;

    public ArticleStore() {
        setExpireTime(TimeRange.WEEK);
    }

    /**
     * Set the global expiration time for this ArticleStore
     * @param range
     * @return
     */
    public int setExpireTime(TimeRange range){
        switch (range){
            case SECOND:    expireTime = 1;             break;
            case MINUTE:    expireTime = 60;            break;
            case HOUR:      expireTime = 60*60;         break;
            case DAY:       expireTime = 60*60*24;      break;
            case WEEK:
            default:        expireTime = 60*60*24*7;    break;
        }
        return expireTime;
    }

    private static int counter = -1;
    public static String gensym(String prefix){
        counter++;
        return prefix+"_"+counter;
    }
    public static String gensym(){
        return gensym("AUTOGENSYM");
    }

    /**
     * Give to correct key format for an article
     * @param id article id
     * @return namespaced key as a string
     */
    public static String articleKey(long id){
        assert(id>=0);
        return "articles:" + id;
    }

    public static String votersKey(long articleId){
        assert (articleId>=0);
        return articleKey(articleId)+":voters";
    }

    public static String categoryKey(String categoryName){
        assert !categoryName.isEmpty();
        return "category:"+categoryName;
    }

    /**
     * Add an article in the Redis databas
     * @param conn A jedis connection instance used to communicate with Redis
     * @param user A username or id
     * @param title Article title
     * @param url Link to this article
     * @return new article id
     */
    public long addArticle(Jedis conn, String user, String title, String url){
        final long articleID = conn.incr("articles:id:last");

        final String articleKey = articleKey(articleID);
        final long now = System.currentTimeMillis() / 1000;

        HashMap<String, String> article = new HashMap<>();
        article.put("title", title);
        article.put("link", url);
        article.put("user", user);
        article.put("timestamp", String.valueOf(now));
        article.put("nbVotes", "1");
        article.put("score", String.valueOf(now+voteIncrement));
        conn.hmset(articleKey, article);


        conn.zadd("timeline", now, articleKey);  // ordered by date
        conn.zadd("scores", now + voteIncrement, articleKey);  // ordered by score


        final String votedSetKey = votersKey(articleID);
        conn.sadd(votedSetKey, user);  // set of users that voted for a particular article
        conn.expire(votedSetKey, expireTime);

        return articleID;
    }


    public boolean addInCategory(Jedis conn, String category, long idArticle){
        boolean alreadyIn = false;

        final String categoryKey = categoryKey(category);
        final String articleKey = articleKey(idArticle);

        alreadyIn = conn.sismember(categoryKey,articleKey);
        if(!alreadyIn){
            conn.sadd(categoryKey, articleKey);
        }
        return !alreadyIn;
    }

    public List<Map<String,String>> getAllByCategory(Jedis conn, String category) throws IOException{
        ArrayList<Map<String,String>> resultColl = new ArrayList<>();

        final String categoryKey = categoryKey(category);
        final String tempKeyName = gensym();

        conn.zinterstore(tempKeyName, categoryKey, "scores");

        ArrayList<Response<Map<String,String>>> responseList = new ArrayList<>();

        Pipeline p = conn.pipelined();

        conn.zrevrange(tempKeyName, 0 , -1)
                .stream()
                .map(p::hgetAll)
                .forEachOrdered(responseList::add);

        p.sync();
        p.close();

        responseList.stream()
                .map(Response::get)
                .forEachOrdered(resultColl::add);

        conn.del(tempKeyName);

        return resultColl;
    }

    /**
     * Try to vote an an article.
     * A user can vote only one time and cannot vote after the expiration time
     * @param conn
     * @param articleId  id of the desired article to vote for.
     * @param user  id of the voting user
     * @return a status stating about the state of the vote.
     */
    public VoteStatus vote(Jedis conn, long articleId, String user) throws IOException {
        VoteStatus status = VoteStatus.UNDEFINED_STATE;

        final String articleKey = articleKey(articleId);
        final String votersKey = votersKey(articleId);

        if(!conn.exists(votersKey)){
            status = VoteStatus.CANNOT_VOTE_ANYMORE;
        }
        else if (conn.sismember(votersKey,user)){
            status = VoteStatus.ALREADY_VOTED;
        }
        else{
            final Transaction t = conn.multi();
            t.sadd(votersKey,user);
            t.hincrBy(articleKey, "nbVotes", 1);
            t.hincrBy(articleKey, "score", voteIncrement);
            t.zincrby("scores",voteIncrement, articleKey);
            t.exec();
            status = VoteStatus.VOTED;
        }
        return status;
    }

    /**
     * Return all articles in the range 0, N. N can be negative.
     * @param conn
     * @param key Key of the zset to perform a zrang onto.
     * @param n
     * @return
     * @throws IOException
     */
    private ArrayList<Map<String,String>> getNInZRevRange(Jedis conn, String key, int n) throws IOException{
        final ArrayList<Map<String, String>> result = new ArrayList<>();
        if(n>0){
            Pipeline p = conn.pipelined();

            List<Response<Map<String, String>>> responseList = conn.zrevrange(key, 0, n)
                    .stream()
                    .map(p::hgetAll)
                    .collect(Collectors.toList());
            p.sync();
            p.close();

            responseList.stream()
                    .map(Response::get)
                    .forEachOrdered(result::add);

        }
        return result;
    }

    /**
     * Get the N latests articles, sorted by timestamp
     * @param conn
     * @param n
     * @return A list of articles, represented as maps
     * @throws IOException
     */
    public List<Map<String, String>> getNLatests(Jedis conn, int n) throws IOException {
        ArrayList<Map<String, String>> result = new ArrayList<>();
        if(n > 0){
            result = getNInZRevRange(conn, "timeline", n);
        }
        return result;
    }

    /**
     * Return a list of the N most upvoted
     * @param conn
     * @param n
     * @return
     * @throws IOException
     */
    public List<Map<String, String>> getNMostUpvoted(Jedis conn, int n) throws IOException{
        ArrayList<Map<String, String>> result = new ArrayList<>();
        if(n > 0){
            result = getNInZRevRange(conn, "scores", n);
        }
        return result;
    }

    public List<Map<String, String>> getAll(Jedis conn) throws IOException{
        return getNInZRevRange(conn, "timeline", -1);
    }
}
