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
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import javax.transaction.UserTransaction;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Ensure that the ignoreJTA Region setting works
 *
 * @since GemFire 4.1.1
 */
@SuppressWarnings("deprecation")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(IntegrationTest.class)
public class JtaNoninvolvementJUnitTest {

  private Cache cache;
  private Region nonTxRegion;
  private Region txRegion;

  private void createCache(boolean copyOnRead) throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    this.cache = CacheFactory.create(DistributedSystem.connect(p));

    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    af.setIgnoreJTA(true);
    this.nonTxRegion = this.cache.createRegion("JtaNoninvolvementJUnitTest", af.create());
    af.setIgnoreJTA(false);
    this.txRegion = this.cache.createRegion("JtaInvolvementTest", af.create());
  }

  private void closeCache() throws CacheException {
    if (this.cache != null) {
      this.txRegion = null;
      this.nonTxRegion = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }
  
  @After
  public void after() {
    closeCache();
    InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }
  
  @Test
  public void test000Noninvolvement() throws Exception {
    try {
      if (cache == null) {
        createCache(false);
      }
      javax.transaction.UserTransaction ut =
        (javax.transaction.UserTransaction)cache.getJNDIContext()
          .lookup("java:/UserTransaction");
      {
        ut.begin();
        txRegion.put("transactionalPut", "xxx");
        assertTrue("expect cache to be in a transaction",
            cache.getCacheTransactionManager().exists());
        ut.commit();
      }
      assertFalse("ensure there is no transaction before testing non-involvement", 
          cache.getCacheTransactionManager().exists());
      {
        ut.begin();
        nonTxRegion.put("nontransactionalPut", "xxx");
        assertFalse("expect cache to not be in a transaction",
          cache.getCacheTransactionManager().exists());
        ut.commit();
      }
    }
    
    finally {
      closeCache();
      cache = null;
    }
  }

  @Test
  public void test001NoninvolvementMultipleRegions_bug45541() throws Exception {
    javax.transaction.UserTransaction ut = null;
    try {
      if (cache == null) {
        createCache(false);
      }
      final CountDownLatch l = new CountDownLatch(1);
      final AtomicBoolean exceptionOccured = new AtomicBoolean(false);
      ut = 
          (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");
      ut.begin();
      txRegion.put("key", "value");
      nonTxRegion.put("key", "value");
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          if (txRegion.get("key") != null) {
            exceptionOccured.set(true);
          }
          if (nonTxRegion.get("key") != null) {
            exceptionOccured.set(true);
          }
          l.countDown();
        }
      });
      t.start();
      l.await();
      assertFalse(exceptionOccured.get());
    } finally {
      if (ut != null) {
        ut.commit();
      }
      closeCache();
      cache = null;
    }
  }

  /**
   * test for gemfire.ignoreJTA flag
   */
  @Test
  public void test002IgnoreJTASysProp() throws Exception {
    javax.transaction.UserTransaction ut = null;
    try {
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "ignoreJTA", "true");
      createCache(false);
      ut = 
          (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");
      ut.begin();
      txRegion.put("key", "value");
      ut.rollback();
      // operation was applied despite the rollback
      assertEquals("value", txRegion.get("key"));
    } finally {
      closeCache();
      cache = null;
      System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "ignoreJTA", "false");
    }
  }
}
