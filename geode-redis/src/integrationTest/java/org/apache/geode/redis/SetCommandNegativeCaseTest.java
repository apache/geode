package org.apache.geode.redis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Random;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.redis.GeodeRedisServer.REDIS_META_DATA_REGION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Category({RedisTest.class})
public class SetCommandNegativeCaseTest {
    private static Jedis jedis;
    private static GeodeRedisServer server;
    private static GemFireCache cache;
    private static Random rand;
    private static int port = 6379;

    @BeforeClass
    public static void setUp() {
        rand = new Random();
        CacheFactory cf = new CacheFactory();
        cf.set(LOG_LEVEL, "error");
        cf.set(MCAST_PORT, "0");
        cf.set(LOCATORS, "");
        cache = cf.create();
        port = AvailablePortHelper.getRandomAvailableTCPPort();
        server = new GeodeRedisServer("localhost", port);

        server.start();
        jedis = new Jedis("localhost", port, 10000000);
    }


    @After
    public void flushAll() {
        jedis.flushAll();
    }

    @AfterClass
    public static void tearDown() {
        jedis.close();
        cache.close();
        server.shutdown();
    }

    @Test
    public void Should_Throw_RedisDataTypeMismatchException_Given_Key_Already_Exists_With_Different_RedisDataType() {
        jedis.hset("key", "field", "value");

        assertThatThrownBy(
                () -> jedis.set("key", "something else")
        ).isInstanceOf(JedisDataException.class)
                .hasMessageContaining("WRONGTYPE");
    }

    @Test
    public void Should_Throw_RedisDataTypeMismatchException_Given_RedisDataType_REDIS_PROTECTED() {
        assertThatThrownBy(
                () -> jedis.set(REDIS_META_DATA_REGION, "something else")
        ).isInstanceOf(JedisDataException.class)
                .hasMessageContaining("protected");
    }
}
