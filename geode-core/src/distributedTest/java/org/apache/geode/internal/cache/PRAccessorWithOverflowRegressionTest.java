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

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * PR accessor configured for OverflowToDisk should not create a diskstore.
 *
 * <p>
 * TRAC #42055: a pr accessor configured for OverflowToDisk fails during creation because of disk
 */

public class PRAccessorWithOverflowRegressionTest extends CacheTestCase {

  private static final int ENTRIES_COUNT = 1;

  private String uniqueName;
  private File datastoreDiskDir;
  private File accessorDiskDir;

  private VM datastore;
  private VM accessor;

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedDiskDirRule diskDirsRule = new DistributedDiskDirRule();

  @Before
  public void setUp() throws Exception {
    datastore = getHost(0).getVM(0);
    accessor = getHost(0).getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();

    datastoreDiskDir = diskDirsRule.getDiskDirFor(datastore);
    accessorDiskDir = diskDirsRule.getDiskDirFor(accessor);

    datastore.invoke(() -> createDataStore());
    accessor.invoke(() -> createAccessor());
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testPROverflow() throws Exception {
    accessor.invoke(() -> {
      Region<String, String> region = getCache().getRegion(uniqueName);
      for (int i = 1; i <= ENTRIES_COUNT + 1; i++) {
        region.put("key-" + i, "value-" + i);
      }

      PartitionedRegion partitionedRegion = (PartitionedRegion) region;
      assertThat(partitionedRegion.getDataStore()).isNull();
      assertThat(partitionedRegion.size()).isGreaterThanOrEqualTo(0);
    });

    datastore.invoke(() -> {
      PartitionedRegion partitionedRegion = (PartitionedRegion) getCache().getRegion(uniqueName);
      assertThat(getCache().getRegion(uniqueName).size()).isEqualTo(2);
      assertThat(partitionedRegion.getDataStore().getAllLocalBucketIds()).hasSize(2);
    });

    // datastore should create diskstore
    await()
        .untilAsserted(() -> assertThat(datastoreDiskDir.listFiles().length).isGreaterThan(0));

    // accessor should not create a diskstore
    assertThat(accessorDiskDir.listFiles()).hasSize(0);
  }

  private void createDataStore() {
    AttributesFactory af = new AttributesFactory();
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setEvictionAttributes(createLRUEntryAttributes(ENTRIES_COUNT, OVERFLOW_TO_DISK));
    af.setPartitionAttributes(new PartitionAttributesFactory().create());

    getCache().createRegion(uniqueName, af.create());
  }

  private void createAccessor() {
    PartitionAttributesFactory<Integer, TestDelta> paf = new PartitionAttributesFactory<>();
    paf.setLocalMaxMemory(0);

    AttributesFactory<Integer, TestDelta> af = new AttributesFactory<>();
    af.setDataPolicy(DataPolicy.PARTITION);
    af.setEvictionAttributes(createLRUEntryAttributes(ENTRIES_COUNT, OVERFLOW_TO_DISK));
    af.setPartitionAttributes(paf.create());

    getCache().createRegion(uniqueName, af.create());
  }
}
