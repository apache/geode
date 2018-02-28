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

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedTestRule;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PRCreationTotalNumBucketsDistributedTest implements Serializable {

  private VM vm0;
  private VM vm1;
  private int totalNumBuckets;

  @ClassRule
  public static DistributedTestRule distributedTestRule = new DistributedTestRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Before
  public void setUp() throws Exception {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);

    totalNumBuckets = 7;
  }

  @Test
  public void testSetTotalNumBuckets() throws Exception {
    vm0.invoke(() -> {
      Cache cache = cacheRule.createCache();

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
      regionFactory.create("PR1");

      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      regionFactory.create("PR2");
    });

    vm1.invoke(() -> {
      Cache cache = cacheRule.createCache();

      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setLocalMaxMemory(0);
      RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      PartitionedRegion accessor = (PartitionedRegion) regionFactory.create("PR1");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(GLOBAL_MAX_BUCKETS_DEFAULT);

      try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
        assertThatThrownBy(() -> regionFactory.create("PR2"))
            .isInstanceOf(IllegalStateException.class);
      }

      partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      accessor = (PartitionedRegion) regionFactory.create("PR2");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(totalNumBuckets);
    });
  }

  @Test
  public void testSetGlobalProperties() throws Exception {
    vm0.invoke(() -> {
      Cache cache = cacheRule.createCache();

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      regionFactory.create("PR1");
    });

    vm1.invoke(() -> {
      Cache cache = cacheRule.createCache();

      RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
      PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

      try (IgnoredException ie = addIgnoredException(IllegalStateException.class)) {
        assertThatThrownBy(() -> regionFactory.create("PR1"))
            .isInstanceOf(IllegalStateException.class);
      }

      Properties globalProperties = new Properties();
      globalProperties.setProperty(GLOBAL_MAX_BUCKETS_PROPERTY, "" + totalNumBuckets);
      partitionAttributesFactory.setGlobalProperties(globalProperties);
      regionFactory.setPartitionAttributes(partitionAttributesFactory.create());
      PartitionedRegion accessor = (PartitionedRegion) regionFactory.create("PR1");

      assertThat(accessor.getTotalNumberOfBuckets()).isEqualTo(totalNumBuckets);
    });
  }
}
