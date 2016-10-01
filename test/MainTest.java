import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import static org.junit.Assert.*;

/**
 * Created by geoffrey on 26/09/2016.
 */
public class MainTest {
    private static final String host = "localhost";
    private static final int port = 6379;
    private static final int db = 2;


    @BeforeClass
    public static void cleanUp(){
        Jedis jedis = new Jedis(host,port);
        jedis.select(db);
        if(db != 0)
            jedis.flushDB();
    }

    @Test
    public void testConnexion(){
        Jedis jedis = new Jedis(host,port);
        jedis.select(db);
        assertEquals(jedis.ping(), "PONG");
    }
}