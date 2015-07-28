package com.gemstone.gemfire.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import redis.clients.jedis.Jedis;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ListsJUnitTest {

  private static Jedis jedis;
  private static GemFireRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() throws IOException {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    //cf.set("log-file", "redis.log");
    cf.set("log-level", "error");
    cf.set("mcast-port", "0");
    cf.set("locators", "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GemFireRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @Test
  public void testLindex() {
    int elements = 50;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);
    

    for (int i = 0; i < elements; i++) {
      String gemString = jedis.lindex(key, i);
      String s = strings.get(i);
      assertEquals(gemString, s);
    }
  }

  @Test
  public void testLPopRPush() {
    int elements = 50;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String gemString = jedis.lpop(key);
      String s = strings.get(i);
      assertEquals(s, gemString);
    }
  }

  @Test
  public void testRPopLPush() {
    int elements = 500;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.lpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String gemString = jedis.rpop(key);
      String s = strings.get(i);
      assertEquals(gemString, s);
    }

  }

  @Test
  public void testLRange() {
    int elements = 10;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      List<String> range = jedis.lrange(key, 0, i);
      assertEquals(range, strings.subList(0, i+1));
    }

    for (int i = 0; i < elements; i++) {
      List<String> range = jedis.lrange(key, i, -1);
      assertEquals(range, strings.subList(i, strings.size()));
    }
  }

  @Test
  public void testLTrim() {
    int elements = 5;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);
    // Take off last element one at a time
    for (int i = elements - 1; i >= 0; i--) {
      jedis.ltrim(key, 0, i);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(range, strings.subList(0, i+1));
    }
    jedis.rpop(key);
    jedis.rpush(key, stringArray);
    // Take off first element one at a time
    for (int i = 1; i < elements; i++) {
      jedis.ltrim(key, 1, -1);
      List<String> range = jedis.lrange(key, 0, -1);
      List<String> expected = strings.subList(i, strings.size());
      assertEquals(range, expected);
    }
  }

  @Test
  public void testLRPushX() {
    String key = randString();
    String otherKey = "Other key";
    jedis.lpush(key, randString());
    assertTrue(jedis.lpushx(key, randString()) > 0);
    assertTrue(jedis.rpushx(key, randString()) > 0);

    assertTrue(jedis.lpushx(otherKey, randString()) == 0);
    assertTrue(jedis.rpushx(otherKey, randString()) == 0);

    jedis.del(key);

    assertTrue(jedis.lpushx(key, randString()) == 0);
    assertTrue(jedis.rpushx(key, randString()) == 0);
  }

  @Test
  public void testLRem() {
    int elements = 5;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String remove = strings.remove(0);
      jedis.lrem(key, 0, remove);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(strings, range);
    }
  }

  @Test
  public void testLSet() {
    int elements = 10;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String s = randString();
      strings.set(i, s);
      jedis.lset(key, i, s);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(range, strings);
    }
  }

  private String randString() {
    int length = rand.nextInt(8) + 5;
    StringBuilder rString = new StringBuilder();
    for (int i = 0; i < length; i++)
      rString.append((char) (rand.nextInt(57) + 65));
    //return rString.toString();
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
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
}
