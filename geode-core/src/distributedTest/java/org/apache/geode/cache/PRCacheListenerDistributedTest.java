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

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.logging.internal.log4j.api.LogService;
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
public class PRCacheListenerDistributedTest extends ReplicateCacheListenerDistributedTest {

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

  @Override
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

  @Override
  protected int expectedCreates() {
    return 1;
  }

  @Override
  protected int expectedUpdates() {
    return 1;
  }

  @Override
  protected int expectedInvalidates() {
    return 1;
  }

  @Override
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

    assertThat(sharedCountersRule.getTotal(REGION_DESTROY)).isEqualTo(expectedRegionDestroys());
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

}
