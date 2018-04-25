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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.*;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.LonerDistributionManager;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.MembershipTest;

/**
 * This class makes sure an isolated "loner" distribution manager can be created and do some cache
 * functions.
 */
@SuppressWarnings("deprecation")
@Category({IntegrationTest.class, MembershipTest.class})
public class LonerDMJUnitTest {

  @After
  public void tearDown() {
    DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if (ds != null) {
      ds.disconnect();
    }
  }

  @Test
  public void testLoner() throws CacheException {
    long start;
    long end;
    DistributedSystem ds = null;
    Cache c = null;
    Properties cfg = new Properties();
    cfg.setProperty(MCAST_PORT, "0");
    cfg.setProperty(LOCATORS, "");
    cfg.setProperty(STATISTIC_SAMPLING_ENABLED, "false");
    cfg.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");

    for (int i = 0; i < 2; i++) {
      start = System.currentTimeMillis();
      ds = DistributedSystem.connect(cfg);
      end = System.currentTimeMillis();
      System.out.println("ds.connect took    " + (end - start) + " ms");
      try {

        start = System.currentTimeMillis();
        c = CacheFactory.create(ds);
        end = System.currentTimeMillis();
        System.out.println("Cache create took " + (end - start) + " ms");

        try {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.GLOBAL);
          Region r = c.createRegion("loner", af.create());
          r.put("key1", "value1");
          r.get("key1");
          r.get("key2");
          r.invalidate("key1");
          r.destroy("key1");
          r.destroyRegion();
        } finally {
          {
            start = System.currentTimeMillis();
            c.close();
            end = System.currentTimeMillis();
            System.out.println("Cache close took " + (end - start) + " ms");
          }
        }
      } finally {
        if (ds != null) {
          start = System.currentTimeMillis();
          ds.disconnect();
          end = System.currentTimeMillis();
          System.out.println("ds.disconnect took " + (end - start) + " ms");
        }
        ds = null;
      }
    }
  }

  @Test
  public void testLonerWithStats() throws CacheException {
    long start;
    long end;
    DistributedSystem ds = null;
    Cache c = null;
    Properties cfg = new Properties();
    cfg.setProperty(MCAST_PORT, "0");
    cfg.setProperty(LOCATORS, "");
    cfg.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    cfg.setProperty(STATISTIC_ARCHIVE_FILE, "lonerStats.gfs");
    cfg.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");

    for (int i = 0; i < 1; i++) {
      start = System.currentTimeMillis();
      ds = DistributedSystem.connect(cfg);
      end = System.currentTimeMillis();
      System.out.println("ds.connect took    " + (end - start) + " ms");
      try {

        start = System.currentTimeMillis();
        c = CacheFactory.create(ds);
        end = System.currentTimeMillis();
        System.out.println("Cache create took " + (end - start) + " ms");

        try {
          AttributesFactory af = new AttributesFactory();
          af.setScope(Scope.GLOBAL);
          Region r = c.createRegion("loner", af.create());
          r.put("key1", "value1");
          r.get("key1");
          r.get("key2");
          r.invalidate("key1");
          r.destroy("key1");
          r.destroyRegion();
        } finally {
          {
            start = System.currentTimeMillis();
            c.close();
            end = System.currentTimeMillis();
            System.out.println("Cache close took " + (end - start) + " ms");
          }
        }
      } finally {
        if (ds != null) {
          start = System.currentTimeMillis();
          ds.disconnect();
          end = System.currentTimeMillis();
          System.out.println("ds.disconnect took " + (end - start) + " ms");
        }
        ds = null;
      }
    }
  }

  @Test
  public void testMemberId() throws UnknownHostException {
    String host = InetAddress.getLocalHost().getCanonicalHostName();
    String name = "Foo";

    Properties cfg = new Properties();
    cfg.setProperty(MCAST_PORT, "0");
    cfg.setProperty(LOCATORS, "");
    cfg.setProperty(ROLES, "lonelyOne");
    cfg.setProperty(NAME, name);
    cfg.setProperty(ENABLE_NETWORK_PARTITION_DETECTION, "false");
    DistributedSystem ds = DistributedSystem.connect(cfg);
    System.out.println("MemberId = " + ds.getMemberId());
    assertEquals(OSProcess.getId(), ds.getDistributedMember().getProcessId());
    if (!PureJavaMode.isPure()) {
      String pid = String.valueOf(OSProcess.getId());
      assertTrue(ds.getMemberId().indexOf(pid) > -1);
    }
    assertTrue(ds.getMemberId().indexOf(name) > -1);
    // make sure the loner port can be updated
    ((LonerDistributionManager) ((InternalDistributedSystem) ds).getDM()).updateLonerPort(100);
  }

}
