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
package org.apache.geode.internal.jta;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.transaction.UserTransaction;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
@RunWith(JUnitParamsRunner.class)
public class SetOperationJTAJUnitTest {
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
  public void tearDown() throws Exception {
    closeCache();
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionKeysetWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartJTA);
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    try {
      userTX.begin();
      Collection<Long> set = region.keySet();
      set.forEach((key) -> assertTrue(testData.keySet().contains(key)));
    } finally {
      validateTXManager(disableSetOpToStartJTA);
      if (!disableSetOpToStartJTA) {
        userTX.rollback();
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionValuesWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartJTA);
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.values().contains(value)));
    } finally {
      validateTXManager(disableSetOpToStartJTA);
      if (!disableSetOpToStartJTA) {
        userTX.rollback();
      }
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionEntriesWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    Region<Long, String> region = setupAndLoadRegion(disableSetOpToStartJTA);
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    try {
      userTX.begin();
      Collection<Map.Entry<Long, String>> set = region.entrySet();
      set.forEach((entry) -> {
        assertTrue(testData.values().contains(entry.getValue()));
        assertTrue(testData.keySet().contains(entry.getKey()));
      });
    } finally {
      validateTXManager(disableSetOpToStartJTA);
      if (!disableSetOpToStartJTA) {
        userTX.rollback();
      }
    }
  }

  private Region<Long, String> setupAndLoadRegion(boolean disableSetOpToStartTx) {
    this.cache = createCache(disableSetOpToStartTx);
    Region<Long, String> region = createRegion(cache);
    testData.forEach((k, v) -> region.put(k, v));
    return region;
  }

  private UserTransaction startUserTransaction(Context ctx) throws Exception {
    return (UserTransaction) ctx.lookup("java:/UserTransaction");
  }

  private void validateTXManager(boolean disableSetOpToStartTx) {
    if (disableSetOpToStartTx) {
      assertNull(TXManagerImpl.getCurrentTXState());
    } else {
      assertNotNull(TXManagerImpl.getCurrentTXState());
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
      (System.currentTimeMillis() % 2 == 0 ? DistributionConfig.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  private Cache createCache(boolean disableSetOpToStartJTA) {
    if (disableSetOpToStartJTA) {
      logger.info("setting system property {} to true ", RESTORE_SET_OPERATION_PROPERTY);
      System.setProperty(RESTORE_SET_OPERATION_PROPERTY, "true");
    }
    CacheFactory cf = new CacheFactory().set(MCAST_PORT, "0");
    this.cache = (GemFireCacheImpl) cf.create();
    return this.cache;
  }

  protected void closeCache() {
    if (this.cache != null) {
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }
}
