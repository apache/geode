package com.gemstone.gemfire.redis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class AuthJUnitTest {

  private static final String PASSWORD = "pwd";
  Jedis jedis;
  GemFireRedisServer server;
  GemFireCache cache;
  Random rand;
  int port;

  int runs = 150;

  @Before
  public void setUp() throws IOException {
    rand = new Random();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    this.jedis = new Jedis("localhost", port, 100000);
  }

  @After
  public void tearDown() throws InterruptedException {
    server.shutdown();
    cache.close();
  }
  private void setupCacheWithPassword() {
    CacheFactory cf = new CacheFactory();
    cf.set("log-level", "error");
    cf.set("mcast-port", "0");
    cf.set("locators", "");
    cf.set("redis-password", PASSWORD);
    cache = cf.create();
    server = new GemFireRedisServer("localhost", port);
    server.start();
  }

  @Test
  public void testAuthConfig() {
    setupCacheWithPassword();
    InternalDistributedSystem iD = (InternalDistributedSystem) cache.getDistributedSystem();
    assert(iD.getConfig().getRedisPassword().equals(PASSWORD));
  }

  @Test
  public void testAuthRejectAccept() {
    setupCacheWithPassword();
    Exception ex = null;
    try {                        
      jedis.auth("wrongpwd");
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);

    String res = jedis.auth(PASSWORD);
    assertEquals(res, "OK");
  }

  @Test
  public void testAuthNoPwd() {
    CacheFactory cf = new CacheFactory();
    cf.set("log-level", "error");
    cf.set("mcast-port", "0");
    cf.set("locators", "");
    cache = cf.create();
    server = new GemFireRedisServer("localhost", port);
    server.start();

    Exception ex = null;
    try {                        
      jedis.auth(PASSWORD);
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);
  }

  @Test
  public void testAuthAcceptRequests() {
    setupCacheWithPassword();
    Exception ex = null;
    try {                        
      jedis.set("foo", "bar");
    } catch (JedisDataException e) {
      ex = e;
    }
    assertNotNull(ex);

    String res = jedis.auth(PASSWORD);
    assertEquals(res, "OK");

    jedis.set("foo", "bar"); // No exception
  }

  @Test
  public void testSeparateClientRequests() {
    setupCacheWithPassword();
    Jedis authorizedJedis = null;
    Jedis nonAuthorizedJedis = null;
    try {
      authorizedJedis =  new Jedis("localhost", port, 100000);
      nonAuthorizedJedis = new Jedis("localhost", port, 100000);
      String res = authorizedJedis.auth(PASSWORD);
      assertEquals(res, "OK");
      authorizedJedis.set("foo", "bar"); // No exception for authorized client

      authorizedJedis.auth(PASSWORD);
      Exception ex = null;
      try {                        
        nonAuthorizedJedis.set("foo", "bar");
      } catch (JedisDataException e) {
        ex = e;
      }
      assertNotNull(ex);
    } finally {
      if (authorizedJedis != null)
        authorizedJedis.close();
      if (nonAuthorizedJedis != null)
        nonAuthorizedJedis.close();
    }
  }

}