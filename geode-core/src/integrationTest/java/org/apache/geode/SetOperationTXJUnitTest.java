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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.util.internal.GeodeGlossary;

@RunWith(GeodeParamsRunner.class)
public class SetOperationTXJUnitTest {

  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "region1";

  private Map<Long, String> testData;
  private Cache cache;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setup() {
    testData = new HashMap<>();
    testData.put(1L, "value1");
    testData.put(2L, "value2");
    testData.put(3L, "duplicateValue");
    testData.put(4L, "duplicateValue");
  }

  @After
  public void tearDownTest() throws Exception {
    closeCache();
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionKeysetWithTx(boolean disableSetOpToStartTx) {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartTx);
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    try {
      txMgr.begin();
      Collection<Long> set = region.keySet();
      set.forEach((key) -> assertTrue(testData.containsKey(key)));
    } finally {
      validateTXManager(disableSetOpToStartTx);
      txMgr.rollback();
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionValuesWithTx(boolean disableSetOpToStartTx) {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartTx);
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    try {
      txMgr.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.containsValue(value)));
    } finally {
      validateTXManager(disableSetOpToStartTx);
      txMgr.rollback();
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionEntriesWithTx(boolean disableSetOpToStartTx) {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartTx);
    CacheTransactionManager txMgr = cache.getCacheTransactionManager();
    try {
      txMgr.begin();
      Collection<Map.Entry<Long, String>> set = region.entrySet();
      set.forEach((entry) -> {
        assertTrue(testData.containsValue(entry.getValue()));
        assertTrue(testData.containsKey(entry.getKey()));
      });
    } finally {
      validateTXManager(disableSetOpToStartTx);
      txMgr.rollback();
    }
  }

  private Region<Long, String> setupAndLoadRegion(boolean disableSetOpToStartTx) {
    cache = createCache(disableSetOpToStartTx);
    Region<Long, String> region = createRegion(cache);
    testData.forEach((k, v) -> region.put(k, v));
    return region;
  }

  private void validateTXManager(boolean disableSetOpToStartTx) {
    assertNotNull(TXManagerImpl.getCurrentTXState());
    if (disableSetOpToStartTx) {
      assertFalse(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
    } else {
      assertTrue(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
    }
  }

  protected Region<Long, String> createRegion(Cache cache) {
    RegionFactory<Long, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region<Long, String> r = rf.create(REGION_NAME);
    return r;
  }

  final String restoreSetOperationTransactionBehavior = "restoreSetOperationTransactionBehavior";
  final String RESTORE_SET_OPERATION_PROPERTY =
      (System.currentTimeMillis() % 2 == 0 ? GeodeGlossary.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  private Cache createCache(boolean disableSetOpToStartTx) {
    if (disableSetOpToStartTx) {
      logger.info("setting system property {} to true ", RESTORE_SET_OPERATION_PROPERTY);
      System.setProperty(RESTORE_SET_OPERATION_PROPERTY, "true");
    }
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");
    cache = cf.create();
    return cache;
  }

  protected void closeCache() {
    if (cache != null) {
      Cache c = cache;
      cache = null;
      c.close();
    }
  }
}
