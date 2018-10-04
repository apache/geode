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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.ExpirationAction.INVALIDATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.Properties;
import java.util.function.Consumer;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.ExpiryTask;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;

/**
 * Tests transaction expiration functionality
 *
 * @since GemFire 4.0
 */
@RunWith(JUnitParamsRunner.class)
public class TXExpirationIntegrationTest {

  private static final String REGION_NAME =
      TXExpirationIntegrationTest.class.getSimpleName() + "_region";
  private static final String KEY = "key";

  private InternalCache cache;
  private CacheTransactionManager transactionManager;

  @Before
  public void setUp() throws Exception {
    cache = (InternalCache) new CacheFactory(getConfig()).create();
    transactionManager = cache.getCacheTransactionManager();
  }

  protected Properties getConfig() {
    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");
    return config;
  }

  @After
  public void tearDown() throws Exception {
    try {
      transactionManager.rollback();
    } catch (IllegalStateException ignore) {
    }
    cache.close();

    ExpiryTask.permitExpiration();
  }

  @Test
  @Parameters({"ENTRY_IDLE_DESTROY", "ENTRY_IDLE_INVALIDATE", "ENTRY_TTL_DESTROY",
      "ENTRY_TTL_INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void entryExpirationDoesNotCauseConflict(ExpirationOperation operation) throws Exception {
    InternalRegion region = createRegion(REGION_NAME);
    AttributesMutator<String, String> mutator = region.getAttributesMutator();

    KeyCacheListener keyCacheListener = spy(new KeyCacheListener());
    mutator.addCacheListener(keyCacheListener);

    ExpiryTask.suspendExpiration();
    operation.mutate(mutator);

    region.put(KEY, "value1");

    transactionManager.begin();
    region.put(KEY, "value2");
    awaitEntryExpiration(region, KEY); // enables expiration
    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value2");

    ExpiryTask.suspendExpiration();
    transactionManager.commit();

    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value2");
    awaitEntryExpiration(region, KEY); // enables expiration
    operation.verifyInvoked(keyCacheListener);

    operation.verifyState(region);
  }

  @Test
  @Parameters({"ENTRY_IDLE_DESTROY", "ENTRY_IDLE_INVALIDATE", "ENTRY_TTL_DESTROY",
      "ENTRY_TTL_INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void entryExpirationContinuesAfterCommitConflict(ExpirationOperation operation)
      throws Exception {
    Region<String, String> region = createRegion(REGION_NAME);

    AttributesMutator<String, String> mutator = region.getAttributesMutator();

    KeyCacheListener keyCacheListener = spy(new KeyCacheListener());
    mutator.addCacheListener(keyCacheListener);

    ExpiryTask.suspendExpiration();
    operation.mutate(mutator);

    ExpiryTask.suspendExpiration();
    region.put(KEY, "value1");

    transactionManager.begin();
    region.put(KEY, "value2");
    awaitEntryExpiration(region, KEY); // enables expiration
    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value2");

    ExpiryTask.suspendExpiration();

    TXManagerImpl txManagerImpl = (TXManagerImpl) transactionManager;
    TXStateProxy txStateProxy = txManagerImpl.pauseTransaction();
    region.put(KEY, "conflict");
    txManagerImpl.unpauseTransaction(txStateProxy);

    assertThatThrownBy(() -> transactionManager.commit())
        .isInstanceOf(CommitConflictException.class);

    awaitEntryExpiration(region, KEY); // enables expiration
    operation.verifyInvoked(keyCacheListener);
    operation.verifyState(region);
  }

  @Test
  @Parameters({"ENTRY_IDLE_DESTROY", "ENTRY_IDLE_INVALIDATE", "ENTRY_TTL_DESTROY",
      "ENTRY_TTL_INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void entryExpirationContinuesAfterRollback(ExpirationOperation operation)
      throws Exception {
    Region<String, String> region = createRegion(REGION_NAME);

    AttributesMutator<String, String> mutator = region.getAttributesMutator();

    KeyCacheListener keyCacheListener = spy(new KeyCacheListener());
    mutator.addCacheListener(keyCacheListener);

    ExpiryTask.suspendExpiration();
    operation.mutate(mutator);

    region.put(KEY, "value1");

    transactionManager.begin();
    region.put(KEY, "value2");
    awaitEntryExpiration(region, KEY); // enables expiration
    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value2");

    ExpiryTask.suspendExpiration();

    transactionManager.rollback();

    awaitEntryExpiration(region, KEY); // enables expiration
    operation.verifyInvoked(keyCacheListener);
    operation.verifyState(region);
  }

  @Test
  @Parameters({"REGION_IDLE_DESTROY", "REGION_IDLE_INVALIDATE", "REGION_TTL_DESTROY",
      "REGION_TTL_INVALIDATE"})
  @TestCaseName("{method}({params})")
  public void regionExpirationDoesNotCauseConflict(ExpirationOperation operation) throws Exception {
    Region<String, String> region = createRegion(REGION_NAME);

    KeyCacheListener keyCacheListener = spy(new KeyCacheListener());

    AttributesMutator<String, String> mutator = region.getAttributesMutator();
    mutator.addCacheListener(keyCacheListener);

    ExpiryTask.suspendExpiration();
    operation.mutate(mutator);

    transactionManager.begin();
    region.put(KEY, "value1");

    awaitRegionExpiration(region); // enables expiration
    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value1");

    ExpiryTask.suspendExpiration();
    transactionManager.commit();

    assertThat(region.getEntry(KEY).getValue()).isEqualTo("value1");

    awaitRegionExpiration(region); // enables expiration
    operation.verifyInvoked(keyCacheListener);
    operation.verifyState(region);
  }

  private enum ExpirationOperation {
    ENTRY_IDLE_DESTROY(m -> m.setEntryIdleTimeout(new ExpirationAttributes(1, DESTROY)),
        s -> verify(s, atLeast(1)).afterDestroyKey(eq(KEY)),
        r -> assertThat(r.containsKey(KEY)).isFalse()),
    ENTRY_IDLE_INVALIDATE(m -> m.setEntryIdleTimeout(new ExpirationAttributes(1, INVALIDATE)),
        s -> verify(s, atLeast(1)).afterInvalidateKey(eq(KEY)),
        r -> assertThat(r.get(KEY)).isNull()),
    ENTRY_TTL_DESTROY(m -> m.setEntryTimeToLive(new ExpirationAttributes(1, DESTROY)),
        s -> verify(s, atLeast(1)).afterDestroyKey(eq(KEY)),
        r -> assertThat(r.containsKey(KEY)).isFalse()),
    ENTRY_TTL_INVALIDATE(m -> m.setEntryTimeToLive(new ExpirationAttributes(1, INVALIDATE)),
        s -> verify(s, atLeast(1)).afterInvalidateKey(eq(KEY)),
        r -> assertThat(r.get(KEY)).isNull()),
    REGION_IDLE_DESTROY(m -> m.setRegionIdleTimeout(new ExpirationAttributes(1, DESTROY)),
        s -> verify(s, atLeast(1)).afterRegionDestroyName(eq(REGION_NAME)),
        r -> await().until(() -> r.isDestroyed())),
    REGION_IDLE_INVALIDATE(m -> m.setRegionIdleTimeout(new ExpirationAttributes(1, INVALIDATE)),
        s -> verify(s, atLeast(1)).afterRegionInvalidateName(eq(REGION_NAME)),
        r -> await().until(() -> r.get(KEY) == null)),
    REGION_TTL_DESTROY(m -> m.setRegionTimeToLive(new ExpirationAttributes(1, DESTROY)),
        s -> verify(s, atLeast(1)).afterRegionDestroyName(eq(REGION_NAME)),
        r -> await().until(() -> r.isDestroyed())),
    REGION_TTL_INVALIDATE(m -> m.setRegionTimeToLive(new ExpirationAttributes(1, INVALIDATE)),
        s -> verify(s, atLeast(1)).afterRegionInvalidateName(eq(REGION_NAME)),
        r -> await().until(() -> r.get(KEY) == null));

    private final Consumer<AttributesMutator> mutatorConsumer;
    private final Consumer<KeyCacheListener> listenerConsumer;
    private final Consumer<Region> regionConsumer;

    ExpirationOperation(Consumer<AttributesMutator> mutatorConsumer,
        Consumer<KeyCacheListener> listenerConsumer, Consumer<Region> regionConsumer) {
      this.mutatorConsumer = mutatorConsumer;
      this.listenerConsumer = listenerConsumer;
      this.regionConsumer = regionConsumer;
    }

    void mutate(AttributesMutator mutator) {
      mutatorConsumer.accept(mutator);
    }

    void verifyInvoked(KeyCacheListener keyCacheListener) {
      listenerConsumer.accept(keyCacheListener);
    }

    public void verifyState(Region region) {
      regionConsumer.accept(region);
    }
  }

  private InternalRegion createRegion(String name) {
    RegionFactory<String, String> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setStatisticsEnabled(true);

    System.setProperty(LocalRegion.EXPIRY_MS_PROPERTY, "true");
    try {
      return (InternalRegion) rf.create(name);
    } finally {
      System.clearProperty(LocalRegion.EXPIRY_MS_PROPERTY);
    }
  }

  private void awaitEntryExpired(Region region, String key) {
    await().until(() -> region.getEntry(key) == null || region.get(key) == null);
  }

  private void awaitEntryExpiration(Region region, String key) {
    InternalRegion internalRegion = (InternalRegion) region;
    try {
      ExpirationDetector detector;
      do {
        detector = new ExpirationDetector(internalRegion.getEntryExpiryTask(key));
        ExpiryTask.expiryTaskListener = detector;
        ExpiryTask.permitExpiration();
        detector.awaitExecuted(30, SECONDS);
      } while (!detector.hasExpired() && detector.wasRescheduled());
    } finally {
      ExpiryTask.expiryTaskListener = null;
    }
  }

  private void awaitRegionExpiration(Region region) {
    InternalRegion internalRegion = (InternalRegion) region;
    try {
      ExpirationDetector detector;
      do {
        detector = new ExpirationDetector(internalRegion.getRegionTTLExpiryTask() != null
            ? internalRegion.getRegionTTLExpiryTask() : internalRegion.getRegionIdleExpiryTask());
        ExpiryTask.expiryTaskListener = detector;
        ExpiryTask.permitExpiration();
        detector.awaitExecuted(30, SECONDS);
      } while (!detector.hasExpired() && detector.wasRescheduled());
    } finally {
      ExpiryTask.expiryTaskListener = null;
    }
  }

  private static class KeyCacheListener extends CacheListenerAdapter<String, String> {

    @Override
    public void afterCreate(EntryEvent<String, String> event) {
      afterCreateKey(event.getKey());
    }

    @Override
    public void afterUpdate(EntryEvent<String, String> event) {
      afterUpdateKey(event.getKey());
    }

    @Override
    public void afterInvalidate(EntryEvent<String, String> event) {
      afterInvalidateKey(event.getKey());
    }

    @Override
    public void afterDestroy(EntryEvent<String, String> event) {
      afterDestroyKey(event.getKey());
    }

    @Override
    public void afterRegionInvalidate(RegionEvent<String, String> event) {
      afterRegionInvalidateName(event.getRegion().getName());
    }

    @Override
    public void afterRegionDestroy(RegionEvent<String, String> event) {
      afterRegionDestroyName(event.getRegion().getName());
    }

    public void afterCreateKey(Object key) {
      // nothing
    }

    public void afterUpdateKey(Object key) {
      // nothing
    }

    public void afterInvalidateKey(Object key) {
      // nothing
    }

    public void afterDestroyKey(Object key) {
      // nothing
    }

    public void afterRegionInvalidateName(Object key) {
      // nothing
    }

    public void afterRegionDestroyName(Object key) {
      // nothing
    }
  }
}
