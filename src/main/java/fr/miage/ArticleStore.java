package fr.miage;

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
 * Create and read articles in a Redis cache.
 */
public class ArticleStore {

    /**
     * Global expire time for expiring keys.
     */
    private static int expireTime;

    /**
     * Constant score increment for a vote.
     */
    private static final int voteIncrement = 457;

    public ArticleStore() {
        setExpireTime(TimeRange.WEEK);
    }

    /**
     * Set the global expiration time for this fr.miage.ArticleStore
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

    /**
     * A counter used to generate unique string
     */
    private static int counter = -1;
    public static String gensym(String prefix){
        counter++;
        return prefix+"_"+counter;
    }

    // Default args
    public static String gensym(){
        return gensym("AUTOGENSYM");
    }

    /**
     * Give the correct key format for an article
     * @param id The article id
     * @return namespaced key as a string
     */
    public static String articleKey(long id){
        assert(id>=0);
        return "articles:" + id;
    }

    /**
     * Give the correct key format for a voter set
     * @param articleId  The article id
     * @return
     */
    public static String votersKey(long articleId){
        assert (articleId>=0);
        return articleKey(articleId)+":voters";
    }

    /**
     * Give the correct key for a category set
     * @param categoryName  The name of the category
     * @return
     */
    public static String categoryKey(String categoryName){
        assert !categoryName.isEmpty();
        return "category:"+categoryName;
    }

    /**
     * Add an article in the Redis database
     * @param conn A jedis connection instance used to communicate with Redis
     * @param user A username or id
     * @param title Article title
     * @param url Link to this article
     * @return new article id
     */
    public long addArticle(Jedis conn, String user, String title, String url){

        // get unique database wide article ID
        final long articleID = conn.incr("articles:id:last");

        final String articleKey = articleKey(articleID);
        final long now = System.currentTimeMillis() / 1000;  // current time

        HashMap<String, String> article = new HashMap<>();  // new article
        article.put("title", title);
        article.put("link", url);
        article.put("user", user);
        article.put("timestamp", String.valueOf(now));
        article.put("nbVotes", "1");
        article.put("score", String.valueOf(now+voteIncrement));
        conn.hmset(articleKey, article);  // store it in Redis


        conn.zadd("timeline", now, articleKey);  // ordered by date
        conn.zadd("scores", now + voteIncrement, articleKey);  // ordered by score


        final String votedSetKey = votersKey(articleID);  // get the voters keys
        conn.sadd(votedSetKey, user);  // set of users that voted for a particular article

        // The spec said : «You cannot vote after a certain amount of time»
        conn.expire(votedSetKey, expireTime);

        return articleID;
    }


    /**
     * Bind an article to a category
     * @param conn
     * @param category Category to add the article to.
     * @param idArticle Article to bind to the category
     * @return
     */
    public boolean addInCategory(Jedis conn, String category, long idArticle){
        boolean alreadyIn = false;  // is it already in? ATM we don't know

        final String categoryKey = categoryKey(category);
        final String articleKey = articleKey(idArticle);

        // It is alreadyIn if it is a member of the category
        alreadyIn = conn.sismember(categoryKey,articleKey);
        if(!alreadyIn){
            // Bind it
            conn.sadd(categoryKey, articleKey);
        }
        return !alreadyIn;
    }

    /**
     * Get all articles by category
     * @param conn
     * @param category the desired category
     * @return all articles in this category
     * @throws IOException
     */
    public List<Map<String,String>> getAllByCategory(Jedis conn, String category) throws IOException{

        // Will return an list of articles
        ArrayList<Map<String,String>> resultColl = new ArrayList<>();

        final String categoryKey = categoryKey(category);
        final String tempKeyName = gensym();

        // Intersection between the category and the scores
        // Storing all tuples (score, id) that belongs to both (by id)
        // in a temporary sorted-set
        conn.zinterstore(tempKeyName, categoryKey, "scores");

        // A list of promises, that will grow as we add commands to the pipeline
        ArrayList<Response<Map<String,String>>> responseList = new ArrayList<>();

        // Start a pipeline
        Pipeline p = conn.pipelined();

        conn.zrevrange(tempKeyName, 0 , -1)  // In reversed order
                .stream()           // ∀ item
                .map(p::hgetAll)    // promise that we will get all of its fields
                .forEachOrdered(responseList::add); // and respectively, from the first to the last,
                                                    // add it the promises list
        // Resolve all promises at once
        p.sync();
        p.close();  // end the pipeline

        responseList.stream()  // ∀ realised promise
                .map(Response::get)  // dereference it
                .forEachOrdered(resultColl::add);   // and respectively, from the first to the last
                                                    // add it to the result list.

        // Clear the temporary sorted-set
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

        // We don't know the vote status yet.
        VoteStatus status = VoteStatus.UNDEFINED_STATE;

        final String articleKey = articleKey(articleId);
        final String votersKey = votersKey(articleId);

        // Is the vote session over?
        if(!conn.exists(votersKey)){
            // Then we cannot vote anymore.
            status = VoteStatus.CANNOT_VOTE_ANYMORE;
        }
        // Has the user already voted ?
        else if (conn.sismember(votersKey,user)){
            // then he cannot vote again.
            status = VoteStatus.ALREADY_VOTED;
        }
        else{
            // Follow this recipe
            final Transaction t = conn.multi();

            // Add the user to the voters for this article
            t.sadd(votersKey,user);
            // Increment the vote counter by 1
            t.hincrBy(articleKey, "nbVotes", 1);
            // Increment the score by the constant amount
            t.hincrBy(articleKey, "score", voteIncrement);
            // Keep a track of it in the sorted scores (by incrementing also)
            t.zincrby("scores",voteIncrement, articleKey);

            // An execute these steps all-in-a-row.
            t.exec();

            // The user just voted.
            status = VoteStatus.VOTED;
        }
        return status;
    }

    /**
     * Return all articles in the range 0, N. *Sorted by descending order*
     * N can be negative.
     * @param conn
     * @param key Key of the zset to perform a zrange onto.
     * @param n The upper bound (-1 for the maximum)
     * @return A list of articles
     * @throws IOException
     */
    private ArrayList<Map<String,String>> getNInZRevRange(Jedis conn, String key, int n) throws IOException{

        // We will return a list of articles
        final ArrayList<Map<String, String>> result = new ArrayList<>();
        // If we have a non-empty range
        if(n>0){
            // Start a pipeline
            Pipeline p = conn.pipelined();

            // In a temporary collection, and in reverse order
            List<Response<Map<String, String>>> responseList = conn.zrevrange(key, 0, n)
                    .stream()           // ∀ item
                    .map(p::hgetAll)    // promise we will get all of its fields
                    .collect(Collectors.toList()); // and add this promise to the list
            p.sync();  // Realise all promises
            p.close(); // end the stream


            responseList.stream()           // ∀ realised promise
                    .map(Response::get)     // dereference it
                    .forEachOrdered(result::add);   // and respectively, from the first to the last
                                                    // add it to the result collection.

        }
        return result;
    }

    /**
     * Get the N latest articles, sorted by timestamp
     * @param conn
     * @param n
     * @return A list of articles, represented as maps
     * @throws IOException
     */
    public List<Map<String, String>> getNLatests(Jedis conn, int n) throws IOException {
        return getNInZRevRange(conn, "timeline", n);
    }

    /**
     * Return a list of the N most popular
     * @param conn
     * @param n
     * @return
     * @throws IOException
     */
    public List<Map<String, String>> getNMostUpvoted(Jedis conn, int n) throws IOException{
        return getNInZRevRange(conn, "scores", n);
    }

    public List<Map<String, String>> getAll(Jedis conn) throws IOException{
        return getNInZRevRange(conn, "timeline", -1);
    }
}
