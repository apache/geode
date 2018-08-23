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

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@SuppressWarnings("serial")
public class PRCreationTotalNumBucketsDistributedTest implements Serializable {

  private VM vm0;
  private VM vm1;
  private int totalNumBuckets;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    vm0 = getVM(0);
    vm1 = getVM(1);

    totalNumBuckets = 7;
  }

  @Test
  public void testSetTotalNumBuckets() throws Exception {
    vm0.invoke(() -> {
      Cache cache = cacheRule.getOrCreateCache();

      RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
      regionFactory.create("PR1");

      PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
      paf.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(paf.create());
      regionFactory.create("PR2");
    });

    vm1.invoke(() -> {
      Cache cache = cacheRule.getOrCreateCache();

      PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
      paf.setLocalMaxMemory(0);
      RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
      regionFactory.setPartitionAttributes(paf.create());
      PartitionedRegion accessor = (PartitionedRegion) regionFactory.create("PR1");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(GLOBAL_MAX_BUCKETS_DEFAULT);

      try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
        assertThatThrownBy(() -> regionFactory.create("PR2"))
            .isInstanceOf(IllegalStateException.class);
      }

      paf.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(paf.create());
      accessor = (PartitionedRegion) regionFactory.create("PR2");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(totalNumBuckets);
    });
  }

  /**
   * Tests {@link PartitionAttributesFactory#setGlobalProperties(Properties)} which is deprecated.
   */
  @Test
  public void testSetGlobalProperties() throws Exception {
    vm0.invoke(() -> {
      Cache cache = cacheRule.getOrCreateCache();

      RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
      PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
      paf.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(paf.create());
      regionFactory.create("PR1");
    });

    vm1.invoke(() -> {
      Cache cache = cacheRule.getOrCreateCache();

      RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
      PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
      regionFactory.setPartitionAttributes(paf.create());

      try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
        assertThatThrownBy(() -> regionFactory.create("PR1"))
            .isInstanceOf(IllegalStateException.class);
      }

      Properties globalProperties = new Properties();
      globalProperties.setProperty(GLOBAL_MAX_BUCKETS_PROPERTY, "" + totalNumBuckets);
      paf.setGlobalProperties(globalProperties);
      regionFactory.setPartitionAttributes(paf.create());
      PartitionedRegion accessor = (PartitionedRegion) regionFactory.create("PR1");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(totalNumBuckets);
    });
  }
}
