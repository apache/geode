/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Ensure that the ignoreJTA Region setting works
 *
 * @since GemFire 4.1.1
 */
@SuppressWarnings("deprecation")
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JtaNoninvolvementJUnitTest {

  private Cache cache;
  private Region nonTxRegion;
  private Region txRegion;

  private void createCache(boolean copyOnRead) throws CacheException {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0"); // loner
    cache = CacheFactory.create(DistributedSystem.connect(p));

    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    af.setIgnoreJTA(true);
    nonTxRegion = cache.createRegion("JtaNoninvolvementJUnitTest", af.create());
    af.setIgnoreJTA(false);
    txRegion = cache.createRegion("JtaInvolvementTest", af.create());
  }

  private void closeCache() throws CacheException {
    if (cache != null) {
      txRegion = null;
      nonTxRegion = null;
      Cache c = cache;
      cache = null;
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
      javax.transaction.UserTransaction ut = (javax.transaction.UserTransaction) cache
          .getJNDIContext().lookup("java:/UserTransaction");
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
      final AtomicBoolean exceptionOccurred = new AtomicBoolean(false);
      ut = (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");
      ut.begin();
      txRegion.put("key", "value");
      nonTxRegion.put("key", "value");
      Thread t = new Thread(() -> {
        if (txRegion.get("key") != null) {
          exceptionOccurred.set(true);
        }
        if (nonTxRegion.get("key") != null) {
          exceptionOccurred.set(true);
        }
        l.countDown();
      });
      t.start();
      l.await();
      assertFalse(exceptionOccurred.get());
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
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "ignoreJTA", "true");
      createCache(false);
      ut = (UserTransaction) cache.getJNDIContext().lookup("java:/UserTransaction");
      ut.begin();
      txRegion.put("key", "value");
      ut.rollback();
      // operation was applied despite the rollback
      assertEquals("value", txRegion.get("key"));
    } finally {
      closeCache();
      cache = null;
      System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "ignoreJTA", "false");
    }
  }
}
