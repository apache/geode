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

import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.Serializable;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Registers a {@code CacheListener} in the Controller and all DUnit VMs. Verifies
 * {@code CacheListener} invocations for {@code Region} operations.
 *
 * <p>
 * Converted from JUnit 3.
 *
 * @since GemFire 2.0
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class ReplicateCacheListenerInvocationTest implements Serializable {

  private static final Logger logger = LogService.getLogger();

  protected static final int ENTRY_VALUE = 0;
  protected static final int UPDATED_ENTRY_VALUE = 1;

  private static final String CREATES = "CREATES";
  private static final String UPDATES = "UPDATES";
  private static final String INVALIDATES = "INVALIDATES";
  private static final String DESTROYS = "DESTROYS";

  protected String regionName;
  private String key;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedCountersRule sharedCountersRule = SharedCountersRule.builder().build();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() throws Exception {
    regionName = getClass().getSimpleName();
    key = "key-1";

    sharedCountersRule.initialize(CREATES);
    sharedCountersRule.initialize(DESTROYS);
    sharedCountersRule.initialize(INVALIDATES);
    sharedCountersRule.initialize(UPDATES);
  }

  @Test
  public void afterCreateIsInvokedInEveryMember() throws Exception {
    CacheListener<String, Integer> listener = new CreateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(key, ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates());
  }

  @Test
  public void afterUpdateIsInvokedInEveryMember() throws Exception {
    CacheListener<String, Integer> listener = new UpdateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(key, ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());
    region.put(key, UPDATED_ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());

    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates());
  }

  @Test
  public void afterInvalidateIsInvokedInEveryMember() throws Exception {
    CacheListener<String, Integer> listener = new InvalidateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(key, 0, cacheRule.getSystem().getDistributedMember());
    region.invalidate(key);

    assertThat(sharedCountersRule.getTotal(INVALIDATES)).isEqualTo(expectedInvalidates());
    assertThat(region.get(key)).isNull();
  }

  @Test
  public void afterDestroyIsInvokedInEveryMember() throws Exception {
    CacheListener<String, Integer> listener = new DestroyCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getHost(0).getVMCount(); i++) {
      getHost(0).getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(key, 0, cacheRule.getSystem().getDistributedMember());
    region.destroy(key);

    assertThat(sharedCountersRule.getTotal(DESTROYS)).isEqualTo(expectedDestroys());
  }

  protected Region<String, Integer> createRegion(final String name,
      final CacheListener<String, Integer> listener) {
    RegionFactory<String, Integer> regionFactory = cacheRule.getCache().createRegionFactory();
    regionFactory.addCacheListener(listener);
    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    return regionFactory.create(name);
  }

  protected int expectedCreates() {
    return getHost(0).getVMCount() + 1;
  }

  protected int expectedUpdates() {
    return getHost(0).getVMCount() + 1;
  }

  protected int expectedInvalidates() {
    return getHost(0).getVMCount() + 1;
  }

  protected int expectedDestroys() {
    return getHost(0).getVMCount() + 1;
  }

  /**
   * Overridden within tests to increment shared counters.
   */
  static abstract class BaseCacheListener extends CacheListenerAdapter<String, Integer>
      implements Serializable {

    @Override
    public void afterCreate(final EntryEvent event) {
      fail("Unexpected listener callback: afterCreate");
    }

    @Override
    public void afterInvalidate(final EntryEvent event) {
      fail("Unexpected listener callback: afterInvalidate");
    }

    @Override
    public void afterDestroy(final EntryEvent event) {
      fail("Unexpected listener callback: afterDestroy");
    }

    @Override
    public void afterUpdate(final EntryEvent event) {
      fail("Unexpected listener callback: afterUpdate");
    }

    @Override
    public void afterRegionInvalidate(final RegionEvent event) {
      fail("Unexpected listener callback: afterRegionInvalidate");
    }
  }

  class CreateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent event) {
      logger.info("Invoking afterCreate on listener; name={}", event.getKey());
      sharedCountersRule.increment(CREATES);

      errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.CREATE));
      errorCollector.checkThat(event.getOldValue(), nullValue());
      errorCollector.checkThat(event.getNewValue(), equalTo(ENTRY_VALUE));

      if (event.getSerializedOldValue() != null) {
        errorCollector.checkThat(event.getSerializedOldValue().getDeserializedValue(),
            equalTo(event.getOldValue()));
      }
      if (event.getSerializedNewValue() != null) {
        errorCollector.checkThat(event.getSerializedNewValue().getDeserializedValue(),
            equalTo(event.getNewValue()));
      }

      logger.info("create event new value is: {}", event.getNewValue());
    }
  }

  class UpdateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent event) {
      // nothing
    }

    @Override
    public void afterUpdate(final EntryEvent event) {
      logger.info("Invoking afterUpdate on listener; name=" + event.getKey());
      sharedCountersRule.increment(UPDATES);

      errorCollector.checkThat(event.getDistributedMember(), equalTo(event.getCallbackArgument()));
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.UPDATE));
      errorCollector.checkThat(event.getOldValue(), anyOf(equalTo(ENTRY_VALUE), nullValue()));
      errorCollector.checkThat(event.getNewValue(), equalTo(UPDATED_ENTRY_VALUE));

      if (event.getSerializedOldValue() != null) {
        errorCollector.checkThat(event.getSerializedOldValue().getDeserializedValue(),
            equalTo(event.getOldValue()));
      }
      if (event.getSerializedNewValue() != null) {
        errorCollector.checkThat(event.getSerializedNewValue().getDeserializedValue(),
            equalTo(event.getNewValue()));
      }
    }
  }

  class InvalidateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent event) {
      // ignore
    }

    @Override
    public void afterInvalidate(final EntryEvent event) {
      logger.info("Invoking tests invalidated listener");
      sharedCountersRule.increment(INVALIDATES);

      if (event.isOriginRemote()) {
        errorCollector.checkThat(event.getDistributedMember(),
            not(cacheRule.getSystem().getDistributedMember()));
      } else {
        errorCollector.checkThat(event.getDistributedMember(),
            equalTo(cacheRule.getSystem().getDistributedMember()));
      }
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.INVALIDATE));
      errorCollector.checkThat(event.getOldValue(), anyOf(equalTo(ENTRY_VALUE), nullValue()));
      errorCollector.checkThat(event.getNewValue(), nullValue());
    }
  }

  class DestroyCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent event) {
      // ignore
    }

    @Override
    public void afterDestroy(final EntryEvent event) {
      logger.info("Invoking objectDestroyed listener");
      sharedCountersRule.increment(DESTROYS);

      if (event.isOriginRemote()) {
        errorCollector.checkThat(event.getDistributedMember(),
            not(cacheRule.getSystem().getDistributedMember()));
      } else {
        errorCollector.checkThat(event.getDistributedMember(),
            equalTo(cacheRule.getSystem().getDistributedMember()));
      }
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.DESTROY));
      errorCollector.checkThat(event.getOldValue(), anyOf(equalTo(ENTRY_VALUE), nullValue()));
      errorCollector.checkThat(event.getNewValue(), nullValue());
    }
  }
}
