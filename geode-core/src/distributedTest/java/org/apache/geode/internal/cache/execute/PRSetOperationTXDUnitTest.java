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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import junitparams.Parameters;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
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
public class PRSetOperationTXDUnitTest extends JUnit4CacheTestCase {

  private static final Logger logger = LogService.getLogger();
  private static final String REGION_NAME = "region1";

  private Map<Long, String> testData;

  private VM accessor = null;
  private VM dataStore1 = null;
  private VM dataStore2 = null;
  private VM dataStore3 = null;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  public PRSetOperationTXDUnitTest() {
    super();
  }

  @Before
  public void setup() {
    testData = new HashMap<>();
    testData.put(1L, "value1");
    testData.put(2L, "value2");
    testData.put(3L, "duplicateValue");
    testData.put(4L, "duplicateValue");
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
    Invoke.invokeInEveryVM(this::verifyNoTxState);
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionKeysetWithTx(boolean disableSetOpToStartTx) throws Exception {
    setupAndLoadRegion(disableSetOpToStartTx);
    verifyRegionKeysetWithTx(disableSetOpToStartTx);
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionValuesWithTx(boolean disableSetOpToStartTx) throws Exception {
    setupAndLoadRegion(disableSetOpToStartTx);
    verifyRegionValuesWithTx(disableSetOpToStartTx);
  }

  @Test
  @Parameters({"true", "false"})
  public void testRegionEntriesWithTx(boolean disableSetOpToStartTx) throws Exception {
    setupAndLoadRegion(disableSetOpToStartTx);
    verifyRegionEntriesWithTx(disableSetOpToStartTx);
  }

  private void setupAndLoadRegion(boolean disableSetOpToStartTx) {
    createRegion(disableSetOpToStartTx);
    dataStore1.invoke(this::loadRegion);
  }

  private void createRegion(boolean disableSetOpToStartTx) {
    accessor.invoke(() -> createCache(disableSetOpToStartTx));
    dataStore1.invoke(() -> createCache(disableSetOpToStartTx));
    dataStore2.invoke(() -> createCache(disableSetOpToStartTx));
    dataStore3.invoke(() -> createCache(disableSetOpToStartTx));

    accessor.invoke(() -> createPR(true));
    dataStore1.invoke(() -> createPR(false));
    dataStore2.invoke(() -> createPR(false));
    dataStore3.invoke(() -> createPR(false));
  }

  private void loadRegion() {
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    testData.forEach(region::put);
  }

  private void verifyRegionKeysetWithTx(boolean disableSetOpToStartTx) {
    accessor.invoke(() -> verifyRegionKeysetWithTx(disableSetOpToStartTx, true));
    dataStore1.invoke(() -> verifyRegionKeysetWithTx(disableSetOpToStartTx, false));
    dataStore2.invoke(() -> verifyRegionKeysetWithTx(disableSetOpToStartTx, false));
    dataStore3.invoke(() -> verifyRegionKeysetWithTx(disableSetOpToStartTx, false));
  }

  private void verifyRegionValuesWithTx(boolean disableSetOpToStartTx) {
    accessor.invoke(() -> verifyRegionValuesWithTx(disableSetOpToStartTx, true));
    dataStore1.invoke(() -> verifyRegionValuesWithTx(disableSetOpToStartTx, false));
    dataStore2.invoke(() -> verifyRegionValuesWithTx(disableSetOpToStartTx, false));
    dataStore3.invoke(() -> verifyRegionValuesWithTx(disableSetOpToStartTx, false));
  }

  private void verifyRegionEntriesWithTx(boolean disableSetOpToStartTx) {
    accessor.invoke(() -> verifyRegionEntriesWithTx(disableSetOpToStartTx, true));
    dataStore1.invoke(() -> verifyRegionEntriesWithTx(disableSetOpToStartTx, false));
    dataStore2.invoke(() -> verifyRegionEntriesWithTx(disableSetOpToStartTx, false));
    dataStore3.invoke(() -> verifyRegionEntriesWithTx(disableSetOpToStartTx, false));
  }

  private void verifyRegionKeysetWithTx(boolean disableSetOpToStartTx, boolean isAccessor) {
    CacheTransactionManager txMgr = basicGetCache().getCacheTransactionManager();
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      txMgr.begin();
      Collection<Long> set = region.keySet();
      set.forEach((key) -> assertTrue(testData.containsKey(key)));
    } finally {
      validateTXManager(disableSetOpToStartTx, isAccessor);
      txMgr.rollback();
    }
  }

  private void verifyRegionValuesWithTx(boolean disableSetOpToStartTx, boolean isAccessor) {
    CacheTransactionManager txMgr = basicGetCache().getCacheTransactionManager();
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      txMgr.begin();
      Collection<String> set = region.values();
      set.forEach((value) -> assertTrue(testData.containsValue(value)));
    } finally {
      validateTXManager(disableSetOpToStartTx, isAccessor);
      txMgr.rollback();
    }
  }

  private void verifyRegionEntriesWithTx(boolean disableSetOpToStartTx, boolean isAccessor) {
    CacheTransactionManager txMgr = basicGetCache().getCacheTransactionManager();
    Region<Long, String> region = basicGetCache().getRegion(SEPARATOR + REGION_NAME);
    try {
      txMgr.begin();
      Collection<Map.Entry<Long, String>> set = region.entrySet();
      set.forEach((entry) -> {
        assertTrue(testData.containsValue(entry.getValue()));
        assertTrue(testData.containsKey(entry.getKey()));
      });
    } finally {
      validateTXManager(disableSetOpToStartTx, isAccessor);
      txMgr.rollback();
    }
  }

  private void validateTXManager(boolean disableSetOpToStartTx, boolean isAccessor) {
    assertNotNull(TXManagerImpl.getCurrentTXState());
    if (disableSetOpToStartTx || isAccessor) {
      assertFalse(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
    } else {
      assertTrue(((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).hasRealDeal());
    }
  }

  private void verifyNoTxState() {
    TXManagerImpl mgr = getCache().getTxManager();
    assertEquals(0, mgr.hostedTransactionsInProgressForTest());
  }

  final String restoreSetOperationTransactionBehavior = "restoreSetOperationTransactionBehavior";
  final String RESTORE_SET_OPERATION_PROPERTY =
      (System.currentTimeMillis() % 2 == 0 ? GeodeGlossary.GEMFIRE_PREFIX : "geode.")
          + restoreSetOperationTransactionBehavior;

  private void createCache(boolean disableSetOpToStartTx) {
    if (disableSetOpToStartTx) {
      logger.info("setting system property {} to true ", RESTORE_SET_OPERATION_PROPERTY);
      System.setProperty(RESTORE_SET_OPERATION_PROPERTY, "true");
    }
    getCache();
  }

  private void createPR(boolean isAccessor) {
    basicGetCache().createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(new PartitionAttributesFactory<>().setTotalNumBuckets(3)
            .setLocalMaxMemory(isAccessor ? 0 : 1).create())
        .create(REGION_NAME);
  }

}
