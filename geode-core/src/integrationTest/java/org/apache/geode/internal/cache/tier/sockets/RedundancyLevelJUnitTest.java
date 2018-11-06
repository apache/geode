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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.geode.distributed.ConfigurationProperties.CACHE_XML_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Tests the proper intialization of redundancyLevel property.
 *
 */
@Category({ClientSubscriptionTest.class})
public class RedundancyLevelJUnitTest {
  private static final String expectedRedundantErrorMsg =
      "Could not find any server to host redundant client queue.";
  private static final String expectedPrimaryErrorMsg =
      "Could not find any server to host primary client queue.";

  final String expected =
      "Could not initialize a primary queue on startup. No queue servers available";

  /** The distributed system */
  DistributedSystem system;

  /** The distributed system */
  Cache cache;

  /**
   * Close the cache and proxy instances for a test and disconnect from the distributed system.
   */
  @After
  public void tearDown() throws Exception {
    if (cache != null) {
      cache.close();
    }
    if (system != null) {

      final String removeExpectedPEM =
          "<ExpectedException action=remove>" + expectedPrimaryErrorMsg + "</ExpectedException>";
      final String removeExpectedREM =
          "<ExpectedException action=remove>" + expectedRedundantErrorMsg + "</ExpectedException>";

      system.getLogWriter().info(removeExpectedPEM);
      system.getLogWriter().info(removeExpectedREM);

      system.disconnect();
    }
  }

  /**
   * Tests that value for redundancyLevel of the failover set is correctly picked via cache-xml
   * file.(Please note that the purpose of this test is to just verify that the value is initialized
   * correctly from cache-xml and so only client is started and the connection-exceptions due to no
   * live servers, which appear as warnings, are ignored.)
   *
   *
   */
  @Test
  public void testRedundancyLevelSetThroughXML() {
    String path = TestUtil.getResourcePath(getClass(), "RedundancyLevelJUnitTest.xml");

    Properties p = new Properties();
    p.setProperty(MCAST_PORT, "0");
    p.setProperty(LOCATORS, "");
    p.setProperty(CACHE_XML_FILE, path);
    final String addExpected = "<ExpectedException action=add>" + expected + "</ExpectedException>";

    system = DistributedSystem.connect(p);
    system.getLogWriter().info(addExpected);

    final String addExpectedPEM =
        "<ExpectedException action=add>" + expectedPrimaryErrorMsg + "</ExpectedException>";
    final String addExpectedREM =
        "<ExpectedException action=add>" + expectedRedundantErrorMsg + "</ExpectedException>";
    system.getLogWriter().info(addExpectedPEM);
    system.getLogWriter().info(addExpectedREM);

    try {

      cache = CacheFactory.create(system);
      assertNotNull("cache was null", cache);
      Region region = cache.getRegion("/root/exampleRegion");
      assertNotNull(region);
      Pool pool = PoolManager.find("clientPool");
      assertEquals("Redundancy level not matching the one specified in cache-xml", 6,
          pool.getSubscriptionRedundancy());
    } finally {
      final String removeExpected =
          "<ExpectedException action=remove>" + expected + "</ExpectedException>";
      system.getLogWriter().info(removeExpected);
    }
  }

}
