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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Collections;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.ThreadUtils;

public class TXReservationMgrJUnitTest {

  DistributedSystem ds;
  Cache c;
  LocalRegion r;
  int commitCount = 0;
  int conflictCount = 0;

  public TXReservationMgrJUnitTest() {}

  @Before
  public void setUp() throws Exception {
    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    ds = DistributedSystem.connect(p);
    c = CacheFactory.create(ds);
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    r = (LocalRegion) c.createRegion("TXReservationMgrJUnitTest", af.create());
  }

  @After
  public void tearDown() throws Exception {
    c.close();
    ds.disconnect();
  }

  private static final int THREAD_COUNT = Integer.getInteger("junit.THREAD_COUNT", 30);
  private static final int KEY_COUNT = Integer.getInteger("junit.KEY_COUNT", 50);

  protected void doThreadBody(final TXReservationMgr mgr) {
    final String tName = Thread.currentThread().getName();
    for (int i = 0; i < KEY_COUNT; i++) {
      final Object key = (long) i;
      final Boolean isEvent = Boolean.TRUE;
      boolean done = false;
      do {
        try {
          IdentityArrayList l = new IdentityArrayList(1);
          TXRegionLockRequestImpl lr = new TXRegionLockRequestImpl(r.getCache(), r);
          lr.addEntryKeys(Collections.singletonMap(key, isEvent));
          l.add(lr);
          mgr.makeReservation(l);
          String v = (String) r.get(key);
          v += "<" + tName + ">";
          r.put(key, v);
          mgr.releaseReservation(l);
          done = true;
          commitCount++;
        } catch (CommitConflictException ex) {
          conflictCount++;
        }
      } while (!done);
    }
  }

  private boolean checkValue(Object key) {
    String value = (String) r.get(key);
    String missing = "";
    for (int i = 0; i < THREAD_COUNT; i++) {
      String tName = "<t" + i + ">";
      if (value.indexOf(tName) == -1) {
        missing += " " + tName;
      }
    }
    if (!(missing.equals(""))) {
      System.out.println("key" + key + " = " + value + " MISSING=" + missing);
      return false;
    } else {
      return true;
    }
  }

  private void doTestMgr(final TXReservationMgr mgr) throws Exception {
    commitCount = 0;
    conflictCount = 0;
    for (int i = 0; i < KEY_COUNT; i++) {
      r.create((long) i, "VAL");
    }
    Thread[] threads = new Thread[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
      threads[i] = new Thread(() -> doThreadBody(mgr), "t" + i);
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
      threads[i].start();
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
      ThreadUtils.join(threads[i], 60 * 1000); // increased from 30 to 60 for parallel junit runs
    }
    int invalidCount = 0;
    for (int i = 0; i < KEY_COUNT; i++) {
      if (!checkValue((long) i)) {
        invalidCount++;
      }
    }
    System.out.println("invalidCount = " + invalidCount);
    System.out.println("commitCount = " + commitCount);
    System.out.println("conflictCount = " + conflictCount);
    if (invalidCount > 0) {
      throw new IllegalStateException("invalidCount=" + invalidCount);
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
