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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.transaction.UserTransaction;

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
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.util.internal.GeodeGlossary;

@RunWith(GeodeParamsRunner.class)
public class SetOperationJTAJUnitTest {
  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "region1";

  private Map<Long, String> testData;
  private Map<Long, String> modifiedData;
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
    modifiedData = new HashMap<>();
    modifiedData.putAll(testData);
    modifiedData.put(5L, "newValue");
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
      set.forEach((key) -> assertTrue(testData.containsKey(key)));
      testData.keySet().forEach((key) -> assertTrue(set.contains(key)));
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
      set.forEach((value) -> assertTrue(testData.containsValue(value)));
      testData.values().forEach((value) -> assertTrue(set.contains(value)));
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
        assertTrue(testData.containsValue(entry.getValue()));
        assertTrue(testData.containsKey(entry.getKey()));
      });
      testData.entrySet().forEach((entry) -> assertTrue(set.contains(entry)));
    } finally {
      validateTXManager(disableSetOpToStartJTA);
      if (!disableSetOpToStartJTA) {
        userTX.rollback();
      }
    }
  }

  private Region<Long, String> setupAndLoadRegion(boolean disableSetOpToStartTx) {
    cache = createCache(disableSetOpToStartTx);
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
      (System.currentTimeMillis() % 2 == 0 ? GeodeGlossary.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  private Cache createCache(boolean disableSetOpToStartJTA) {
    if (disableSetOpToStartJTA) {
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

  @Test
  public void testRegionValuesWithPutWhenSetOperationStartsJTA() throws Exception {
    setupAndLoadRegion(false);
    verifyRegionValuesWhenSetOperationStartsJTA();
  }

  private void verifyRegionValuesWhenSetOperationStartsJTA() throws Exception {
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = cache.getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.containsValue(value)));
      testData.values().forEach((value) -> assertTrue(set.contains(value)));
      assertEquals(testData.size(), set.size());
      region.put(5L, "newValue");
      set.forEach((value) -> assertTrue(modifiedData.containsValue(value)));
      modifiedData.values().forEach((value) -> assertTrue(set.contains(value)));
      assertEquals(modifiedData.size(), set.size());
    } finally {
      userTX.rollback();
    }
  }

  @Test
  public void testRegionValuesWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    setupAndLoadRegion(true);
    verifyRegionValuesWhenSetOperationDoesNotStartJTA();
  }

  private void verifyRegionValuesWhenSetOperationDoesNotStartJTA() throws Exception {
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = cache.getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.containsValue(value)));
      testData.values().forEach((value) -> assertTrue(set.contains(value)));
      assertEquals(testData.size(), set.size());
      region.put(5L, "newValue");
      assertThatThrownBy(() -> set.contains("newValue")).isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "The Region collection is not transactional but is being used in a transaction");
    } finally {
      userTX.rollback();
    }
  }

  @Test
  public void testTxFunctionOnMemberWhenSetOperationDoesNotStartJTA() {
    doTestTxFunction(true);
  }

  @Test
  public void testTxFunctionOnMemberWhenSetOperationStartsJTA() {
    doTestTxFunction(false);
  }

  private void doTestTxFunction(boolean disableSetOpToStartJTA) {
    setupAndLoadRegion(disableSetOpToStartJTA);
    registerFunction();
    doTxFunction(disableSetOpToStartJTA);
  }

  class TXFunctionSetOpStartsJTA implements Function {
    static final String id = "TXFunctionSetOpStartsJTA";

    @Override
    public void execute(FunctionContext context) {
      Region r = null;
      try {
        verifyRegionValuesWhenSetOperationStartsJTA();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  class TXFunctionSetOpDoesNoStartJTA implements Function {
    static final String id = "TXFunctionSetOpDoesNotStartJTA";

    @Override
    public void execute(FunctionContext context) {
      Region r = null;
      try {
        verifyRegionValuesWhenSetOperationDoesNotStartJTA();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      context.getResultSender().lastResult(Boolean.TRUE);
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean optimizeForWrite() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

  private void registerFunction() {
    FunctionService.registerFunction(new TXFunctionSetOpDoesNoStartJTA());
    FunctionService.registerFunction(new TXFunctionSetOpStartsJTA());
  }

  private void doTxFunction(boolean disableSetOpToStartJTA) {
    DistributedMember owner = cache.getDistributedSystem().getDistributedMember();
    if (disableSetOpToStartJTA) {
      FunctionService.onMember(owner).execute(TXFunctionSetOpDoesNoStartJTA.id).getResult();
    } else {
      FunctionService.onMember(owner).execute(TXFunctionSetOpStartsJTA.id).getResult();
    }
  }

}
