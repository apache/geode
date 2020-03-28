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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * junit test for detecting read conflicts
 */
public class TXDetectReadConflictJUnitTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @Rule
  public TestName name = new TestName();

  private Cache cache = null;
  private Region region = null;
  private Region regionPR = null;
  private final CountDownLatch allowWriteTransactionToCommitLatch = new CountDownLatch(1);
  private final CountDownLatch allowReadTransactionToProceedLatch = new CountDownLatch(1);
  private static final String key = "key";
  private static final String key1 = "key1";
  private static final String value = "value";
  private static final String value1 = "value1";
  private static final String newValue = "newValue";
  private static final String newValue1 = "newValue1";

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "detectReadConflicts", "true");
    createCache();
  }

  protected void createCache() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = new CacheFactory(props).create();
    region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("testRegionRR");
  }

  protected void createCachePR() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = new CacheFactory(props).create();
    regionPR = cache.createRegionFactory(RegionShortcut.PARTITION).create("testRegionPR");
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  @Test
  public void testReadConflictsRR() {
    cache.close();
    createCache();
    region.put(key, value);
    region.put(key1, value1);
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals(value, region.get(key));
    assertEquals(value1, region.get(key1));
    mgr.commit();
  }

  @Test
  public void testReadConflictsPR() {
    cache.close();
    createCachePR();
    regionPR.put(key, value);
    regionPR.put(key1, value1);
    TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
    mgr.begin();
    assertEquals(value, regionPR.get(key));
    assertEquals(value1, regionPR.get(key1));
    mgr.commit();
  }


  /**
   * Test that two transactions with read and put operations produce CommitConflictException
   */

  @Test
  public void readConflictsTransactionCanBlockWriteTransaction() {
    cache.close();
    createCache();

    region.put(key, value);
    region.put(key1, value1);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertThat(region.get(key)).isSameAs(value);
    region.put(key1, newValue1);
    TXState txState =
        (TXState) ((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).getRealDeal(null, null);
    txState.setAfterReservation(() -> readTransactionAfterReservation());
    executorServiceRule.submit(() -> doPutOnReadKeyTransaction());
    txManager.commit();
    assertThat(region.get(key)).isSameAs(value);
    assertThat(region.get(key1)).isSameAs(newValue1);
  }

  private void readTransactionAfterReservation() {
    allowWriteTransactionToCommitLatch.countDown();
    try {
      allowReadTransactionToProceedLatch.await(GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void doPutOnReadKeyTransaction() throws Exception {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    region.put(key, newValue); // expect commit conflict
    allowWriteTransactionToCommitLatch.await(GeodeAwaitility.getTimeout().toMillis(),
        TimeUnit.MILLISECONDS);
    assertThatThrownBy(() -> txManager.commit()).isExactlyInstanceOf(CommitConflictException.class);
    allowReadTransactionToProceedLatch.countDown();
  }

  @Test
  public void readConflictsTransactionCanDetectStateChange() throws Exception {
    cache.close();
    createCache();

    region.put(key, value);
    region.put(key1, value1);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertThat(region.get(key)).isSameAs(value);
    region.put(key1, newValue1);
    executorServiceRule.submit(() -> doPutTransaction());
    allowReadTransactionToProceedLatch.await();
    // expect commit conflict
    assertThatThrownBy(() -> txManager.commit()).isExactlyInstanceOf(CommitConflictException.class);
    assertThat(region.get(key)).isSameAs(newValue);
    assertThat(region.get(key1)).isSameAs(value1);
  }

  private void doPutTransaction() {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    region.put(key, newValue);
    txManager.commit();
    allowReadTransactionToProceedLatch.countDown();
  }

  @Test
  public void readConflictsTransactionCanBlockReadTransaction() {
    cache.close();
    createCachePR();

    regionPR.put(key, value);
    regionPR.put(key1, value1);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertEquals(regionPR.get(key), value);
    regionPR.put(key1, newValue1);
    TXState txState =
        (TXState) ((TXStateProxyImpl) TXManagerImpl.getCurrentTXState()).getRealDeal(null, null);
    txState.setAfterReservation(() -> putTransactionAfterReservation());
    executorServiceRule.submit(() -> doReadonPutKeyTransaction());
    txManager.commit();
    assertEquals(regionPR.get(key), value);
    assertEquals(regionPR.get(key1), newValue1);
  }

  private void putTransactionAfterReservation() {
    allowReadTransactionToProceedLatch.countDown();
    try {
      allowWriteTransactionToCommitLatch.await(GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void doReadonPutKeyTransaction() {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertEquals(regionPR.get(key1), value1);
    try {
      allowReadTransactionToProceedLatch.await(GeodeAwaitility.getTimeout().toMillis(),
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertThatThrownBy(() -> txManager.commit()).isExactlyInstanceOf(CommitConflictException.class);
    allowWriteTransactionToCommitLatch.countDown();
  }

  /**
   * Test that two transactions with only read operations don't produce CommitConflictException
   */

  @Test
  public void readConflictsTransactionNoConflicts() throws Exception {
    cache.close();
    createCachePR();

    regionPR.put(key, value);
    regionPR.put(key1, value1);
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertEquals(regionPR.get(key), value);
    assertEquals(regionPR.get(key1), value1);
    executorServiceRule.submit(() -> doGetTransaction());
    allowReadTransactionToProceedLatch.await();
    txManager.commit();
    assertEquals(regionPR.get(key), value);
    assertEquals(regionPR.get(key1), value1);
  }

  private void doGetTransaction() {
    TXManagerImpl txManager = (TXManagerImpl) cache.getCacheTransactionManager();
    txManager.begin();
    assertEquals(regionPR.get(key), value);
    assertEquals(regionPR.get(key1), value1);
    txManager.commit();
    allowReadTransactionToProceedLatch.countDown();
  }

}
