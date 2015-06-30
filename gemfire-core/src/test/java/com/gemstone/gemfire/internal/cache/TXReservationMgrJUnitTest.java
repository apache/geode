/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import dunit.DistributedTestCase;

@Category(IntegrationTest.class)
public class TXReservationMgrJUnitTest {

  DistributedSystem ds;
  Cache c;
  LocalRegion r;
  int commitCount = 0;
  int conflictCount = 0;
  
  public TXReservationMgrJUnitTest() {
  }
  
  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    p.setProperty("locators", "");
    this.ds = DistributedSystem.connect(p);
    this.c = CacheFactory.create(this.ds);
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    this.r = (LocalRegion)c.createRegion("TXReservationMgrJUnitTest", af.create());
  }
  
  @After
  public void tearDown() throws Exception {
      this.c.close();
      this.ds.disconnect();
  }

  private final static int THREAD_COUNT = Integer.getInteger("junit.THREAD_COUNT", 30).intValue();
  private final static int KEY_COUNT = Integer.getInteger("junit.KEY_COUNT", 50).intValue();

  protected void doThreadBody(final TXReservationMgr mgr) {
    final String tName = Thread.currentThread().getName();
    for (int i=0; i < KEY_COUNT; i++) {
      final Object key = new Long(i);
      boolean done = false;
      do {
        try {
          IdentityArrayList l = new IdentityArrayList(1);
          TXRegionLockRequestImpl lr = new TXRegionLockRequestImpl(this.r);
          lr.addEntryKeys(Collections.singleton(key));
          l.add(lr);
          mgr.makeReservation(l);
          String v = (String)this.r.get(key);
          v += "<" + tName + ">";
          this.r.put(key, v);
          mgr.releaseReservation(l);
          done = true;
          this.commitCount++;
        } catch (CommitConflictException ex) {
          this.conflictCount++;
        }
      } while (!done);
    }
  }
  private boolean checkValue(Object key) {
    String value = (String)this.r.get(key);
    String missing = "";
    for (int i=0; i < THREAD_COUNT; i++) {
      String tName = "<t" + i + ">";
      if (value.indexOf(tName) == -1) {
        missing += " " + tName;
      }
    }
    if (!(missing.equals(""))) {
      System.out.println("key"+key+" = " + value + " MISSING=" + missing);
      return false;
    } else {
      return true;
    }
  }
  private void doTestMgr(final TXReservationMgr mgr) throws Exception {
    this.commitCount = 0;
    this.conflictCount = 0;
    for (int i=0; i < KEY_COUNT; i++) {
      this.r.create(new Long(i), "VAL");
    }
    Thread[] threads = new Thread[THREAD_COUNT];
    for (int i=0; i < THREAD_COUNT; i++) {
      threads[i] = new Thread(new Runnable() {
          public void run() {
            doThreadBody(mgr);
          }
        }, "t"+i);
    }
    for (int i=0; i < THREAD_COUNT; i++) {
      threads[i].start();
    }
    for (int i=0; i < THREAD_COUNT; i++) {
      DistributedTestCase.join(threads[i], 60 * 1000, null); // increased from 30 to 60 for parallel junit runs
    }
    int invalidCount = 0;
    for (int i=0; i < KEY_COUNT; i++) {
      if (!checkValue(new Long(i))) {
        invalidCount++;
      }
    }
    System.out.println("invalidCount = " + invalidCount);
    System.out.println("commitCount = " + this.commitCount);
    System.out.println("conflictCount = " + this.conflictCount);
    if (invalidCount > 0) {
      throw new IllegalStateException("invalidCount="+invalidCount);
    }
  }
  @Test
  public void testLocalResMgr() throws Exception {
    doTestMgr(new TXReservationMgr(true));
  }
  @Test
  public void testNonLocalResMgr() throws Exception {
    doTestMgr(new TXReservationMgr(false));
  }
}
