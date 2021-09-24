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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.Region.SEPARATOR;
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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxyImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;
import org.apache.geode.util.internal.GeodeGlossary;


@RunWith(GeodeParamsRunner.class)
@SuppressWarnings("serial")
public class PRSetOperationJTADUnitTest extends JUnit4CacheTestCase {

  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "region1";

  private Map<Long, String> testData;
  private Map<Long, String> modifiedData;

  private VM accessor = null;
  private VM dataStore1 = null;
  private VM dataStore2 = null;
  private VM dataStore3 = null;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public PRSetOperationJTADUnitTest() {
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
    accessor = host.getVM(3);
  }

  @Override
  public final void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> verifyNoTxState());
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionKeysetWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    setupAndLoadRegion(disableSetOpToStartJTA);
    verifyRegionKeysetWithJTA(disableSetOpToStartJTA);
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionValuesWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    setupAndLoadRegion(disableSetOpToStartJTA);
    verifyRegionValuesWithJTA(disableSetOpToStartJTA);
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionEntriesWithJTA(boolean disableSetOpToStartJTA) throws Exception {
    setupAndLoadRegion(disableSetOpToStartJTA);
    verifyRegionEntriesWithJTA(disableSetOpToStartJTA);
  }

  private void setupAndLoadRegion(boolean disableSetOpToStartJTA) {
    createRegion(disableSetOpToStartJTA);
    dataStore1.invoke(() -> loadRegion());
  }

  private void createRegion(boolean disableSetOpToStartJTA) {
    accessor.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore1.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore2.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore3.invoke(() -> createCache(disableSetOpToStartJTA));

    accessor.invoke(() -> createPR(true));
    dataStore1.invoke(() -> createPR(false));
    dataStore2.invoke(() -> createPR(false));
    dataStore3.invoke(() -> createPR(false));
  }

  private void loadRegion() {
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    testData.forEach((k, v) -> region.put(k, v));
  }

  private void verifyRegionKeysetWithJTA(boolean disableSetOpToStartJTA) {
    accessor.invoke(() -> verifyRegionKeysetWithJTA(disableSetOpToStartJTA, true));
    dataStore1.invoke(() -> verifyRegionKeysetWithJTA(disableSetOpToStartJTA, false));
    dataStore2.invoke(() -> verifyRegionKeysetWithJTA(disableSetOpToStartJTA, false));
    dataStore3.invoke(() -> verifyRegionKeysetWithJTA(disableSetOpToStartJTA, false));
  }

  private void verifyRegionValuesWithJTA(boolean disableSetOpToStartJTA) {
    accessor.invoke(() -> verifyRegionValuesWithJTA(disableSetOpToStartJTA, true));
    dataStore1.invoke(() -> verifyRegionValuesWithJTA(disableSetOpToStartJTA, false));
    dataStore2.invoke(() -> verifyRegionValuesWithJTA(disableSetOpToStartJTA, false));
    dataStore3.invoke(() -> verifyRegionValuesWithJTA(disableSetOpToStartJTA, false));
  }

  private void verifyRegionEntriesWithJTA(boolean disableSetOpToStartJTA) {
    accessor.invoke(() -> verifyRegionEntriesWithJTA(disableSetOpToStartJTA, true));
    dataStore1.invoke(() -> verifyRegionEntriesWithJTA(disableSetOpToStartJTA, false));
    dataStore2.invoke(() -> verifyRegionEntriesWithJTA(disableSetOpToStartJTA, false));
    dataStore3.invoke(() -> verifyRegionEntriesWithJTA(disableSetOpToStartJTA, false));
  }

  private void verifyRegionKeysetWithJTA(boolean disableSetOpToStartJTA, boolean isAccessor)
      throws Exception {
    Context ctx = basicGetCache().getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<Long> set = region.keySet();
      set.forEach((key) -> assertTrue(testData.keySet().contains(key)));
      testData.keySet().forEach((key) -> assertTrue(set.contains(key)));
    } finally {
      validateTXManager(disableSetOpToStartJTA, isAccessor);
      if (!disableSetOpToStartJTA && !isAccessor) {
        userTX.rollback();
      }
    }
  }

  private void verifyRegionValuesWithJTA(boolean disableSetOpToStartJTA, boolean isAccessor)
      throws Exception {
    Context ctx = basicGetCache().getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.values().contains(value)));
      testData.values().forEach((value) -> assertTrue(set.contains(value)));
    } finally {
      validateTXManager(disableSetOpToStartJTA, isAccessor);
      if (!disableSetOpToStartJTA && !isAccessor) {
        userTX.rollback();
      }
    }
  }

  private void verifyRegionEntriesWithJTA(boolean disableSetOpToStartJTA, boolean isAccessor)
      throws Exception {
    Context ctx = basicGetCache().getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<Map.Entry<Long, String>> set = region.entrySet();
      set.forEach((entry) -> {
        assertTrue(testData.values().contains(entry.getValue()));
        assertTrue(testData.keySet().contains(entry.getKey()));
      });
      testData.entrySet().forEach((entry) -> assertTrue(set.contains(entry)));
    } finally {
      validateTXManager(disableSetOpToStartJTA, isAccessor);
      if (!disableSetOpToStartJTA && !isAccessor) {
        userTX.rollback();
      }
    }
  }

  private void validateTXManager(boolean disableSetOpToStartTx, boolean isAccessor) {
    if (disableSetOpToStartTx) {
      assertNull(TXManagerImpl.getCurrentTXState());
    } else {
      assertNotNull(TXManagerImpl.getCurrentTXState());
      if (!isAccessor) {
        assertTrue(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
      }
    }
  }

  private UserTransaction startUserTransaction(Context ctx) throws Exception {
    return (UserTransaction) ctx.lookup("java:/UserTransaction");
  }

  private void verifyNoTxState() {
    TXManagerImpl mgr = getCache().getTxManager();
    assertEquals(0, mgr.hostedTransactionsInProgressForTest());
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

  private void createPR(boolean isAccessor) {
    basicGetCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory<Long, String>().setTotalNumBuckets(3)
            .setLocalMaxMemory(isAccessor ? 0 : 1).create())
        .create(REGION_NAME);
  }

  @Test
  public void testRegionValuesWithPutWhenSetOperationStartsJTA() throws Exception {
    boolean disableSetOpToStartJTA = false;
    setupRegion(disableSetOpToStartJTA);

    accessor.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
    dataStore1.invoke(() -> verifyRegionValuesWhenSetOperationStartsJTA());
  }

  private void setupRegion(boolean disableSetOpToStartJTA) {
    accessor.invoke(() -> createCache(disableSetOpToStartJTA));
    dataStore1.invoke(() -> createCache(disableSetOpToStartJTA));
    accessor.invoke(() -> createPR(true));
    dataStore1.invoke(() -> createPR(false));
    dataStore1.invoke(() -> loadRegion());
  }


  private void verifyRegionValuesWhenSetOperationStartsJTA() throws Exception {
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.values().contains(value)));
      testData.values().forEach((value) -> assertTrue(set.contains(value)));
      assertEquals(testData.size(), set.size());
      region.put(5L, "newValue");
      set.forEach((value) -> assertTrue(modifiedData.values().contains(value)));
      modifiedData.values().forEach((value) -> assertTrue(set.contains(value)));
      assertEquals(modifiedData.size(), set.size());
    } finally {
      userTX.rollback();
    }
  }

  @Test
  public void testRegionValuesWithPutWhenSetOperationDoesNotStartJTA() throws Exception {
    boolean disableSetOpToStartJTA = true;
    setupRegion(disableSetOpToStartJTA);

    accessor.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
    dataStore1.invoke(() -> verifyRegionValuesWhenSetOperationDoesNotStartJTA());
  }


  private void verifyRegionValuesWhenSetOperationDoesNotStartJTA() throws Exception {
    Context ctx = cache.getJNDIContext();
    UserTransaction userTX = startUserTransaction(ctx);
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      userTX.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.values().contains(value)));
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

    accessor.invoke(() -> registerFunction());
    dataStore1.invoke(() -> registerFunction());
    dataStore2.invoke(() -> registerFunction());
    dataStore3.invoke(() -> registerFunction());

    accessor.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore1.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore2.invoke(() -> doTxFunction(disableSetOpToStartJTA));
    dataStore3.invoke(() -> doTxFunction(disableSetOpToStartJTA));
  }

  private void registerFunction() {
    FunctionService.registerFunction(new TXFunctionSetOpDoesNoStartJTA());
    FunctionService.registerFunction(new TXFunctionSetOpStartsJTA());
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

  private void doTxFunction(boolean disableSetOpToStartJTA) {
    PartitionedRegion region =
        (PartitionedRegion) basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    DistributedMember owner = region.getOwnerForKey(region.getKeyInfo(5L));
    if (disableSetOpToStartJTA) {
      FunctionService.onMember(owner).execute(TXFunctionSetOpDoesNoStartJTA.id).getResult();
    } else {
      FunctionService.onMember(owner).execute(TXFunctionSetOpStartsJTA.id).getResult();
    }
  }
}
