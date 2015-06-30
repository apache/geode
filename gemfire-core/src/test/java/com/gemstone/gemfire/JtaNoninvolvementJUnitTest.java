/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import static org.junit.Assert.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.naming.NamingException;
import javax.transaction.UserTransaction;

import org.junit.After;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

/**
 * Ensure that the ignoreJTA Region setting works
 *
 * @author Bruce Schuchardt
 * @since 4.1.1
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
    p.setProperty("mcast-port", "0"); // loner
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
      System.setProperty("gemfire.ignoreJTA", "true");
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
      System.setProperty("gemfire.ignoreJTA", "false");
    }
  }
}
