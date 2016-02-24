/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * junit test for suspend and resume methods
 */
@Category(IntegrationTest.class)
public class TXManagerImplJUnitTest {
  
  @Rule 
  public TestName name = new TestName();

  protected Cache cache = null;
  protected Region region = null;
  
  @Before
  public void setUp() throws Exception {
    createCache();
  }
  
  protected void createCache() {
    Properties props = new Properties();
    props.put("mcast-port", "0");
    props.put("locators", "");
    cache = new CacheFactory(props).create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegion");
  }
  
  @After
  public void tearDown() throws Exception {
    cache.close();
  }
  
  /**
   * two threads suspend and resume a single transaction, while
   * making changes. 
   * @throws Exception
   */
  @Test
  public void testSuspendResume() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    region.put("key", "value");
    assertEquals("value", region.get("key"));
    final TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    try {
      mgr.resume(txId);
      fail("expected ex not thrown");
    } catch (IllegalStateException e) {
    }
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          mgr.resume(txId);
          fail("expected exception not thrown");
        } catch (IllegalStateException e) {
        }
        assertFalse(mgr.tryResume(txId));
        latch.countDown();
        assertTrue(mgr.tryResume(txId, 100, TimeUnit.MILLISECONDS));
        assertEquals("value1", region.get("key1"));
        region.put("key2", "value2");
        latch2.countDown();
        assertEquals(txId, mgr.suspend());
      }
    });
    t.start();
    latch.await();
    region.put("key1", "value1");
    assertEquals(txId, mgr.suspend());
    latch2.await();
    assertTrue(mgr.tryResume(txId, 100, TimeUnit.MILLISECONDS));
    assertEquals("value2", region.get("key2"));
    t.join();
    mgr.commit();
    assertEquals(3, region.size());
  }
  
  @Test
  public void testResumeTimeout() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    final CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(new Runnable() {
      public void run() {
        assertFalse(mgr.tryResume(txId, 1, TimeUnit.SECONDS));
        latch.countDown();
      }
    });
    long start = System.currentTimeMillis();
    t.start();
    latch.await();
    long end = System.currentTimeMillis();
    // other thread waits for 1 second
    assertTrue(end - start > 950);
    t.join();
    mgr.commit();
  }
  
  @Test
  public void testMultipleSuspends() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);
    final CountDownLatch latch3 = new CountDownLatch(1);
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        latch2.countDown();
        assertTrue(mgr.tryResume(txId, 1, TimeUnit.SECONDS));
        assertNull(region.get("key2"));
        region.put("key1", "value1");
        assertEquals(txId, mgr.suspend());
      }
    });
    Thread t2 = new Thread(new Runnable() {
      public void run() {
        latch3.countDown();
        assertTrue(mgr.tryResume(txId, 1, TimeUnit.SECONDS));
        assertEquals("value1", region.get("key1"));
        region.put("key2", "value");
        assertEquals(txId, mgr.suspend());
        latch.countDown();
      }
    });
    t1.start();
    latch2.await();
    Thread.sleep(200);
    t2.start();
    latch3.await();
    Thread.sleep(200);
    mgr.suspend();
    if (!latch.await(30, TimeUnit.SECONDS)) {
      fail("junit test failed");
    }
    mgr.tryResume(txId, 1, TimeUnit.SECONDS);
    assertEquals(3, region.size());
    mgr.commit();
  }
  
  @Test
  public void testUnblockOnCommit() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    final CountDownLatch latch = new CountDownLatch(2);
    final CountDownLatch latch2 = new CountDownLatch(2);
    Thread t1 = new Thread(new Runnable() {
      public void run() {
        latch.countDown();
        assertFalse(mgr.tryResume(txId, 10, TimeUnit.SECONDS));
        latch2.countDown();
      }
    });
    Thread t2 = new Thread(new Runnable() {
      public void run() {
        latch.countDown();
        assertFalse(mgr.tryResume(txId, 10, TimeUnit.SECONDS));
        latch2.countDown();
      }
    });
    t1.start();
    t2.start();
    latch.await();
    Thread.sleep(100);
    long start = System.currentTimeMillis();
    mgr.commit();
    assertTrue("expected to wait for less than 100 millis, but waited for:"
        +(System.currentTimeMillis() - start), latch2.await(100, TimeUnit.MILLISECONDS));
    t1.join();
    t2.join();
  }
  
  @Test
  public void testExists() {
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    TransactionId txId = mgr.suspend();
    assertTrue(mgr.exists(txId));
    mgr.resume(txId);
    assertTrue(mgr.exists(txId));
    mgr.commit();
    assertFalse(mgr.exists(txId));
    
    mgr.begin();
    txId = mgr.suspend();
    assertTrue(mgr.exists(txId));
    mgr.resume(txId);
    assertTrue(mgr.exists(txId));
    mgr.rollback();
    assertFalse(mgr.exists(txId));
  }
  
  @Test
  public void testEarlyoutOnTryResume() {
    CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    mgr.commit();
    
    long start = System.currentTimeMillis();
    mgr.tryResume(txId, 10, TimeUnit.SECONDS);
    assertTrue("did not expect tryResume to block", System.currentTimeMillis() - start < 100);
  }

  /**
   * test that timeout of Long.MAX_VALUE does not return immediately
   * @throws Exception
   */
  @Test
  public void testWaitForever() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    mgr.resume(txId);
    final CountDownLatch latch = new CountDownLatch(1);
    Thread t = new Thread(new Runnable() {
      public void run() {
        long start = System.currentTimeMillis();
        assertTrue(mgr.tryResume(txId, Long.MAX_VALUE, TimeUnit.MILLISECONDS));
        long end = System.currentTimeMillis();
        assert end - start >= 1000;
        latch.countDown();
      }
    });
    t.start();
    Thread.sleep(1100);
    mgr.suspend();
    latch.await();
  }

  @Test
  public void testResumeCommitWithExpiry_bug45558() throws Exception {
    final CacheTransactionManager mgr = cache.getCacheTransactionManager();
    final CountDownLatch l = new CountDownLatch(1);
    region.close();
    AttributesFactory<String, String> af = new AttributesFactory<String, String>();
    af.setStatisticsEnabled(true);
    af.setEntryIdleTimeout(new ExpirationAttributes(5));
    //region.getAttributesMutator().setEntryTimeToLive(new ExpirationAttributes(5));
    region = cache.createRegion(name.getMethodName(), af.create());
    region.put("key", "value");
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        mgr.resume(txId);
        mgr.commit();
        l.countDown();
      }
    });
    t.start();
    l.await();
  }

  @Test
  public void testSuspendTimeout() throws Exception {
    cache.close();
    System.setProperty("gemfire.suspendedTxTimeout", "1");
    createCache();
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    assertEquals(1, mgr.getSuspendedTransactionTimeout());
    mgr.begin();
    region.put("key", "value");
    final TransactionId txId = mgr.suspend();
    Thread.sleep(70*1000);
    try {
      mgr.resume(txId);
      fail("An expected exception was not thrown");
    } catch (IllegalStateException expected) {
    }
    assertNull(region.get("key"));
    System.setProperty("gemfire.suspendedTxTimeout", "");
  }
}
