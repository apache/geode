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
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.SharedCountersRule;
import org.apache.geode.test.dunit.rules.SharedErrorCollector;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * This class tests event triggering and handling in partitioned regions.
 *
 * <p>
 * Converted from JUnit 3.
 *
 * @since GemFire 5.1
 */

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class PRCacheListenerDistributedTest implements Serializable {

  protected static final String CLEAR = "CLEAR";
  protected static final String REGION_DESTROY = "REGION_DESTROY";
  private static final String CREATES = "CREATES";
  private static final String UPDATES = "UPDATES";
  private static final String INVALIDATES = "INVALIDATES";
  private static final String DESTROYS = "DESTROYS";
  private static final int ENTRY_VALUE = 0;
  private static final int UPDATED_ENTRY_VALUE = 1;
  private static final String KEY = "key-1";
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
  protected String regionName;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {1, Boolean.FALSE},
        {3, Boolean.TRUE},
    });
  }

  @Parameter
  public int redundancy;

  @Parameter(1)
  public Boolean withData;

  protected Region<String, Integer> createRegion(final String name,
      final CacheListener<String, Integer> listener) {
    return createPartitionedRegion(name, listener, false);
  }

  protected Region<String, Integer> createAccessorRegion(final String name,
      final CacheListener<String, Integer> listener) {
    return createPartitionedRegion(name, listener, true);
  }

  private Region<String, Integer> createPartitionedRegion(String name,
      CacheListener<String, Integer> listener, boolean accessor) {
    LogService.getLogger()
        .info("Params [Redundancy: " + redundancy + " withData:" + withData + "]");
    PartitionAttributesFactory<String, Integer> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundancy);

    if (accessor) {
      paf.setLocalMaxMemory(0);
    }
    RegionFactory<String, Integer> regionFactory = cacheRule.getCache().createRegionFactory();
    if (listener != null) {
      regionFactory.addCacheListener(listener);
    }
    regionFactory.setDataPolicy(DataPolicy.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    return regionFactory.create(name);
  }

  private void withData(Region region) {
    if (withData) {
      // Fewer buckets.
      // Covers case where node doesn't have any buckets depending on redundancy.
      region.put("key1", "value1");
      region.put("key2", "value2");
    }
  }

  protected int expectedCreates() {
    return 1;
  }

  protected int expectedUpdates() {
    return 1;
  }

  protected int expectedInvalidates() {
    return 1;
  }

  protected int expectedDestroys() {
    return 1;
  }

  @Test
  public void afterRegionDestroyIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, listener));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY))
        .isGreaterThanOrEqualTo(expectedRegionDestroys());
  }

  @Test
  public void afterRegionDestroyIsInvokedOnNodeWithListener() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY)).isEqualTo(1);
  }

  @Test
  public void afterRegionDestroyIsInvokedOnRemoteNodeWithListener() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, null);

    getVM(0).invoke(() -> {
      createRegion(regionName, listener);
    });

    for (int i = 1; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY)).isEqualTo(1);
  }

  @Test
  public void afterRegionDestroyIsInvokedOnAccessorAndDataMembers() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, listener));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY))
        .isGreaterThanOrEqualTo(expectedRegionDestroys());
  }

  @Test
  public void afterRegionDestroyIsInvokedOnAccessor() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY)).isEqualTo(1);
  }

  @Test
  public void afterRegionDestroyIsInvokedOnNonAccessor() {
    CacheListener<String, Integer> listener = new RegionDestroyCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, null);
    getVM(0).invoke(() -> {
      createRegion(regionName, listener);
    });
    for (int i = 1; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.destroyRegion();

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY)).isEqualTo(1);
  }

  @Test
  public void afterRegionClearIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, listener));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(expectedClears());
  }

  @Test
  public void afterClearIsInvokedOnNodeWithListener() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(1);
  }

  @Test
  public void afterRegionClearIsInvokedOnRemoteNodeWithListener() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, null);
    getVM(0).invoke(() -> {
      createRegion(regionName, listener);
    });
    for (int i = 1; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(1);
  }

  @Test
  public void afterRegionClearIsInvokedOnAccessorAndDataMembers() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, listener));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(expectedClears());
  }

  @Test
  public void afterRegionClearIsInvokedOnAccessor() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, listener);

    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(1);
  }

  @Test
  public void afterRegionClearIsInvokedOnNonAccessor() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createAccessorRegion(regionName, null);

    getVM(0).invoke(() -> {
      createRegion(regionName, listener);
    });
    for (int i = 1; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        withData(createRegion(regionName, null));
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(1);
  }

  @Before
  public void setUp() {
    regionName = getClass().getSimpleName();

    sharedCountersRule.initialize(CREATES);
    sharedCountersRule.initialize(DESTROYS);
    sharedCountersRule.initialize(INVALIDATES);
    sharedCountersRule.initialize(UPDATES);
    sharedCountersRule.initialize(CLEAR);
    sharedCountersRule.initialize(REGION_DESTROY);
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

  @Test
  public void afterClearIsInvokedInEveryMember() {
    CacheListener<String, Integer> listener = new ClearCountingCacheListener();
    Region<String, Integer> region = createRegion(regionName, listener);
    for (int i = 0; i < getVMCount(); i++) {
      getVM(i).invoke(() -> {
        createRegion(regionName, listener);
      });
    }

    region.clear();

    assertThat(sharedCountersRule.getTotal(CLEAR)).isEqualTo(expectedClears());
  }

  protected int expectedClears() {
    return getVMCount() + 1;
  }

  protected int expectedRegionDestroys() {
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
      sharedCountersRule.increment(CREATES);
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Integer> event) {
      sharedCountersRule.increment(UPDATES);
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

  protected class ClearCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      sharedCountersRule.increment(CREATES);
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Integer> event) {
      sharedCountersRule.increment(UPDATES);
    }

    @Override
    public void afterRegionClear(RegionEvent<String, Integer> event) {

      sharedCountersRule.increment(CLEAR);
      if (!event.getRegion().getAttributes().getDataPolicy().withPartitioning()) {
        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(),
              not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(),
              equalTo(cacheRule.getSystem().getDistributedMember()));
        }
      }
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.REGION_CLEAR));
      errorCollector.checkThat(event.getRegion().getName(), equalTo(regionName));
    }
  }

  protected class RegionDestroyCountingCacheListener extends BaseCacheListener {

    @Override
    public void afterCreate(final EntryEvent<String, Integer> event) {
      sharedCountersRule.increment(CREATES);
    }

    @Override
    public void afterUpdate(final EntryEvent<String, Integer> event) {
      sharedCountersRule.increment(UPDATES);
    }

    @Override
    public void afterRegionDestroy(final RegionEvent<String, Integer> event) {
      sharedCountersRule.increment(REGION_DESTROY);

      if (!event.getRegion().getAttributes().getDataPolicy().withPartitioning()) {
        if (event.isOriginRemote()) {
          errorCollector.checkThat(event.getDistributedMember(),
              not(cacheRule.getSystem().getDistributedMember()));
        } else {
          errorCollector.checkThat(event.getDistributedMember(),
              equalTo(cacheRule.getSystem().getDistributedMember()));
        }
      }
      errorCollector.checkThat(event.getOperation(), equalTo(Operation.REGION_DESTROY));
      errorCollector.checkThat(event.getRegion().getName(), equalTo(regionName));
    }
  }
}
