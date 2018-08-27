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
package org.apache.geode.cache;

import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMCount;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Verifies {@code CacheListener} invocations for {@code Region} operations in multiple members.
 *
 * <p>
 * Converted from JUnit 3.
 *
 * @since GemFire 2.0
 */
@SuppressWarnings("serial")
public class ReplicateCacheListenerDistributedTest implements Serializable {

  private static final String CREATES = "CREATES";
  private static final String UPDATES = "UPDATES";
  private static final String INVALIDATES = "INVALIDATES";
  private static final String DESTROYS = "DESTROYS";

  private static final int ENTRY_VALUE = 0;
  private static final int UPDATED_ENTRY_VALUE = 1;

  private static final String KEY = "key-1";

  private String regionName;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SharedCountersRule sharedCountersRule = new SharedCountersRule();

  @Rule
  public SharedErrorCollector errorCollector = new SharedErrorCollector();

  @Before
  public void setUp() {
    regionName = getClass().getSimpleName();

    sharedCountersRule.initialize(CREATES);
    sharedCountersRule.initialize(DESTROYS);
    sharedCountersRule.initialize(INVALIDATES);
    sharedCountersRule.initialize(UPDATES);
  }

  @Test
  public void afterCreateIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new CreateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(KEY, ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());

    assertThat(sharedCountersRule.getTotal(CREATES)).isEqualTo(expectedCreates());
  }

  @Test
  public void afterUpdateIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new UpdateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(KEY, ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());
    region.put(KEY, UPDATED_ENTRY_VALUE, cacheRule.getSystem().getDistributedMember());

    assertThat(sharedCountersRule.getTotal(UPDATES)).isEqualTo(expectedUpdates());
  }

  @Test
  public void afterInvalidateIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new InvalidateCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(KEY, 0, cacheRule.getSystem().getDistributedMember());
    region.invalidate(KEY);

    assertThat(sharedCountersRule.getTotal(INVALIDATES)).isEqualTo(expectedInvalidates());
    assertThat(region.get(KEY)).isNull();
  }

  @Test
  public void afterDestroyIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new DestroyCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.put(KEY, 0, cacheRule.getSystem().getDistributedMember());
    region.destroy(KEY);

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
    return getVMCount() + 1;
  }

  protected int expectedUpdates() {
    return getVMCount() + 1;
  }

  protected int expectedInvalidates() {
    return getVMCount() + 1;
  }

  protected int expectedDestroys() {
    return getVMCount() + 1;
  }

  /**
   * Overridden within tests to increment shared counters.
   */
  private abstract static class BaseCacheListener extends CacheListenerAdapter<String, Integer>
      implements Serializable {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      fail("Unexpected listener callback: afterCreate");
    }

    @Override
    public void afterInvalidate(final EntryEvent<String, Integer> event) {
      fail("Unexpected listener callback: afterInvalidate");
    }

    @Override
    public void afterDestroy(final EntryEvent<String, Integer> event) {
      fail("Unexpected listener callback: afterDestroy");
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Integer> event) {
      fail("Unexpected listener callback: afterUpdate");
    }

    @Override
    public void afterRegionInvalidate(final RegionEvent<String, Integer> event) {
      fail("Unexpected listener callback: afterRegionInvalidate");
    }
  }

  private class CreateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
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
    }
  }

  private class UpdateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      // nothing
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Integer> event) {
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

  private class InvalidateCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      // ignore
    }

    @Override
    public void afterInvalidate(final EntryEvent<String, Integer> event) {
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

  private class DestroyCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      // ignore
    }

    @Override
    public void afterDestroy(final EntryEvent<String, Integer> event) {
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
