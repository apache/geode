package com.gemstone.gemfire.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import redis.clients.jedis.Jedis;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class StringsJunitTest {

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
  public void testAppendAndStrlen() {
    String key = randString();
    int len = key.length();
    String full = key;
    jedis.set(key, key);
    for (int i = 0; i < 15; i++) {
      String rand = randString();
      jedis.append(key, rand);
      len += rand.length();
      full += rand;
    }
    String ret = jedis.get(key);
    assertTrue(ret.length() == len);
    assertTrue(full.equals(ret));
    assertTrue(full.length() == jedis.strlen(key));
  }

  @Test
  public void testDecr() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, ""+num1);
    //jedis.set(key3, "-100");
    jedis.set(key2, ""+num2);

    jedis.decr(key1);
    jedis.decr(key3);
    jedis.decr(key2);
    assertTrue(jedis.get(key1).equals("" + (num1 - 1)));
    assertTrue(jedis.get(key2).equals("" + (num2 - 1)));
    assertTrue(jedis.get(key3).equals("" + (-1)));
  }

  @Test
  public void testIncr() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, ""+num1);
    //jedis.set(key3, "-100");
    jedis.set(key2, ""+num2);

    jedis.incr(key1);
    jedis.incr(key3);
    jedis.incr(key2);

    assertTrue(jedis.get(key1).equals("" + (num1 + 1)));
    assertTrue(jedis.get(key2).equals("" + (num2 + 1)));
    assertTrue(jedis.get(key3).equals("" + (+1)));
  }

  @Test
  public void testDecrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int decr1 = rand.nextInt(100);
    int decr2 = rand.nextInt(100);
    Long decr3 = Long.MAX_VALUE/2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, ""+num1);
    jedis.set(key2, ""+num2);
    jedis.set(key3, ""+Long.MIN_VALUE);

    jedis.decrBy(key1, decr1);
    jedis.decrBy(key2, decr2);

    assertTrue(jedis.get(key1).equals("" + (num1 - decr1*1)));
    assertTrue(jedis.get(key2).equals("" + (num2 - decr2*1)));

    Exception ex= null;
    try {
      jedis.decrBy(key3, decr3);
    } catch(Exception e) {
      ex = e;
    }
    assertNotNull(ex);

  }  

  @Test
  public void testIncrBy() {
    String key1 = randString();
    String key2 = randString();
    String key3 = randString();
    int incr1 = rand.nextInt(100);
    int incr2 = rand.nextInt(100);
    Long incr3 = Long.MAX_VALUE/2;
    int num1 = 100;
    int num2 = -100;
    jedis.set(key1, ""+num1);
    jedis.set(key2, ""+num2);
    jedis.set(key3, ""+Long.MAX_VALUE);

    jedis.incrBy(key1, incr1);
    jedis.incrBy(key2, incr2);
    assertTrue(jedis.get(key1).equals("" + (num1 + incr1*1)));
    assertTrue(jedis.get(key2).equals("" + (num2 + incr2*1)));

    Exception ex= null;
    try {
      jedis.incrBy(key3, incr3);
    } catch(Exception e) {
      ex = e;
    }
    assertNotNull(ex);
  }

  @Test
  public void testGetRange() {
    String sent = randString();
    String contents = randString();
    jedis.set(sent, contents);
    for (int i = 0; i < sent.length(); i++) {
      String range = jedis.getrange(sent, i, -1);
      assertTrue(contents.substring(i).equals(range));
    }
    assertNull(jedis.getrange(sent, 2,0));
  }

  @Test
  public void testGetSet() {
    String key = randString();
    String contents = randString();
    jedis.set(key, contents);
    String newContents = randString();
    String oldContents = jedis.getSet(key, newContents);
    assertTrue(oldContents.equals(contents));
    contents = newContents;
  }

  @Test
  public void testMSetAndGet() {
    int r = 5;
    String[] keyvals = new String[(r*2)];
    String[] keys = new String[r];
    String[] vals = new String[r];
    for(int i = 0; i < r; i++) {
      String key = randString();
      String val = randString();
      keyvals[2*i] = key;
      keyvals[2*i+1] = val;
      keys[i] = key;
      vals[i] = val;
    }

    jedis.mset(keyvals);

    List<String> ret = jedis.mget(keys);
    Object[] retArray =  ret.toArray();

    assertTrue(Arrays.equals(vals, retArray));
  }

  @Test
  public void testMSetNX() {
    Set<String> strings = new HashSet<String>();
    for(int i = 0; i < 2 * 5; i++)
      strings.add(randString());
    String[] array = strings.toArray(new String[0]);
    long response = jedis.msetnx(array);

    assertTrue(response == 1);

    long response2 = jedis.msetnx(array[0], randString());

    assertTrue(response2 == 0);
    assertEquals(array[1], jedis.get(array[0]));
  }

  @Test
  public void testSetNX() {
    String key1 = randString();
    String key2;
    do {
      key2 = randString();
    } while (key2.equals(key1));

    long response1 = jedis.setnx(key1, key1);
    long response2 = jedis.setnx(key2, key2);
    long response3 = jedis.setnx(key1, key2);

    assertTrue(response1 == 1);
    assertTrue(response2 == 1);
    assertTrue(response3 == 0);
  }

  @Test
  public void testPAndSetex() {
    Random r = new Random();
    int setex = r.nextInt(5);
    if (setex == 0)
      setex = 1;
    String key = randString();
    jedis.setex(key, setex, randString());
    try {
      Thread.sleep((setex  + 5) * 1000);
    } catch (InterruptedException e) {
      return;
    }
    String result = jedis.get(key);
    //System.out.println(result);
    assertNull(result);

    int psetex = r.nextInt(5000);
    if (psetex == 0)
      psetex = 1;
    key = randString();
    jedis.psetex(key, psetex, randString());
    long start = System.currentTimeMillis();
    try {
      Thread.sleep(psetex + 5000);
    } catch (InterruptedException e) {
      return;
    }
    long stop = System.currentTimeMillis();
    result = jedis.get(key);
    assertTrue(stop - start >= psetex);
    assertNull(result);
  }

  private String randString() {
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
