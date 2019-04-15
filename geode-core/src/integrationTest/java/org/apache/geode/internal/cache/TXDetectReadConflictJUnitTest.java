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
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;


/**
 * junit test for detecting read conflicts
 */
public class TXDetectReadConflictJUnitTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TestName name = new TestName();

  protected Cache cache = null;
  protected Region region = null;
  protected Region regionpr = null;


  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "detectReadConflicts", "true");
    createCache();
  }

  protected void createCache() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    cache = new CacheFactory(props).create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegionRR");
  }

  protected void createCachePR() {
    Properties props = new Properties();
    props.put(MCAST_PORT, "0");
    props.put(LOCATORS, "");
    cache = new CacheFactory(props).create();
    regionpr = cache.createRegionFactory(RegionShortcut.PARTITION).create("testRegionPR");
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testReadConflictsRR() throws Exception {
    cache.close();
    createCache();
    region.put("key", "value");
    region.put("key1", "value1");
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals("value", region.get("key"));
    assertEquals("value1", region.get("key1"));
    mgr.commit();
  }

  @Test
  public void testReadConflictsPR() throws Exception {
    cache.close();
    createCachePR();
    regionpr.put("key", "value");
    regionpr.put("key1", "value1");
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals("value", regionpr.get("key"));
    assertEquals("value1", regionpr.get("key1"));
    mgr.commit();
  }
}
