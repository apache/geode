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

package org.apache.geode.internal.cache.tx;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import javax.transaction.UserTransaction;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.util.internal.GeodeGlossary;


public class SetOperationJTADistributedTest extends JUnit4CacheTestCase {
  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "region1";

  private Map<Long, String> testData;
  private Map<Long, String> modifiedData;

  private VM dataStore1 = null;
  private VM dataStore2 = null;
  private VM dataStore3 = null;
  private VM dataStore4 = null;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public SetOperationJTADistributedTest() {
    super();
  }

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

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS(); // isolate this test from others to avoid periodic CacheExistsExceptions
    Host host = Host.getHost(0);
    dataStore1 = host.getVM(0);
    dataStore2 = host.getVM(1);
    dataStore3 = host.getVM(2);
    dataStore4 = host.getVM(3);
  }

  @Test
  public void testRegionValuesWithPutWhenSetOperationStartsJTA() throws Exception {
    setupAndLoadRegion(false);
    dataStore1.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
    dataStore2.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
    dataStore3.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
    dataStore4.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
  }

  private void setupAndLoadRegion(boolean disableSetOpToStartJTA) {
    createRegion(disableSetOpToStartJTA);
    dataStore1.invoke(() -> loadRegion());
  }

  private void createRegion(boolean disableSetOpToStartJTA) {
    dataStore1.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore2.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore3.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore4.invoke(() -> createCache(disableSetOpToStartJTA));

    dataStore1.invoke(() -> createRegion());
    dataStore2.invoke(() -> createRegion());
    dataStore3.invoke(() -> createRegion());
    dataStore4.invoke(() -> createRegion());
  }

  final String restoreSetOperationTransactionBehavior = "restoreSetOperationTransactionBehavior";
  final String RESTORE_SET_OPERATION_PROPERTY =
      (System.currentTimeMillis() % 2 == 0 ? GeodeGlossary.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  private void createCache(boolean disableSetOpToStartJTA) {
    if (disableSetOpToStartJTA) {
      logger.info("setting system property {} to true ", RESTORE_SET_OPERATION_PROPERTY);
      System.setProperty(RESTORE_SET_OPERATION_PROPERTY, "true");
    }
    getCache();
  }

  private void createRegion() {
    RegionFactory<Long, String> rf = basicGetCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region<Long, String> r = rf.create(REGION_NAME);
  }

  private void loadRegion() {
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    testData.forEach((k, v) -> region.put(k, v));
  }

  private void verifyRegionValuesWhenSetOperationStartsJTA() throws Exception {
    Context ctx = getCache().getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = getCache().getRegion(SEPARATOR + REGION_NAME);
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

  private UserTransaction startUserTransaction(Context ctx) throws Exception {
    return (UserTransaction) ctx.lookup("java:/UserTransaction");
  }

  @Test
  public void testRegionValuesWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    setupAndLoadRegion(true);
    dataStore1.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
    dataStore2.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
    dataStore3.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
    dataStore4.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
  }

  private void verifyRegionValuesWhenSetOperationDoesNotStartJTA() throws Exception {
    Context ctx = getCache().getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = getCache().getRegion(SEPARATOR + REGION_NAME);
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
    dataStore1.invoke(() -> registerFunction());
    dataStore2.invoke(() -> registerFunction());
    dataStore3.invoke(() -> registerFunction());
    dataStore4.invoke(() -> registerFunction());

    dataStore1.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore2.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore3.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore4.invoke(() -> doTxFunction(disableSetOpToStartJTA));
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
    DistributedMember owner = getCache().getDistributedSystem().getDistributedMember();
    if (disableSetOpToStartJTA) {
      FunctionService.onMember(owner).execute(TXFunctionSetOpDoesNoStartJTA.id).getResult();
    } else {
      FunctionService.onMember(owner).execute(TXFunctionSetOpStartsJTA.id).getResult();
    }
  }

}
