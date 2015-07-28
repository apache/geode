package com.gemstone.gemfire.redis;

import static org.junit.Assert.assertFalse;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class ConcurrentStartTest {

  int numServers = 10;
  @Test
  public void testCachelessStart() throws InterruptedException {
    runNServers(numServers);
    GemFireCacheImpl.getInstance().close();
  }
  @Test
  public void testCachefulStart() throws InterruptedException {
    CacheFactory cf = new CacheFactory();
    cf.set("mcast-port", "0");
    cf.set("locators", "");
    Cache c = cf.create();
    runNServers(numServers);
    c.close();
  }
  
  private void runNServers(int n) throws InterruptedException {
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(numServers);
    final Thread[] threads = new Thread[n];
    for (int i = 0; i < n; i++) {
      final int j = i;
      Runnable r = new Runnable() {

        @Override
        public void run() {
          GemFireRedisServer s = new GemFireRedisServer(ports[j]);
          s.start();
          s.shutdown();
        }
      };
      
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.start();
      threads[i] = t;
    }
    for (Thread t : threads)
      t.join();
    Cache c = GemFireCacheImpl.getInstance();
    assertFalse(c.isClosed());
  }
}
