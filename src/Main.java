import redis.clients.jedis.Jedis;

public class Main {

    public static final void main(String[] args) {
        Jedis conn = new Jedis("localhost", 6379);

    }
}
