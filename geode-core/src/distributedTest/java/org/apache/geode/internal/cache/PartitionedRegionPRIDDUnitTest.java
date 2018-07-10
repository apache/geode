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

import static org.apache.geode.internal.cache.PartitionedRegion.RETRY_TIMEOUT_PROPERTY;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

/**
 * Tests PRID (PartitionedRegion Id) metadata.
 */

public class PartitionedRegionPRIDDUnitTest extends CacheTestCase {

  private static final String RETRY_TIMEOUT_PROPERTY_VALUE = "20000";

  private String regionName1;
  private String regionName2;
  private int localMaxMemory;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    regionName1 = "PR-1";
    regionName2 = "PR-2";
    localMaxMemory = 200;

    System.setProperty(RETRY_TIMEOUT_PROPERTY, RETRY_TIMEOUT_PROPERTY_VALUE);
  }

  @Test
  public void testPRIDGenerationInMultiplePartitionRegion() throws Exception {
    // create PR-1
    vm0.invoke(() -> createPartitionedRegion(regionName1, localMaxMemory, 0));
    vm1.invoke(() -> createPartitionedRegion(regionName1, localMaxMemory, 0));
    vm2.invoke(() -> createPartitionedRegion(regionName1, localMaxMemory, 0));

    // create PR-2
    vm0.invoke(() -> createPartitionedRegion(regionName2, localMaxMemory, 1));
    vm1.invoke(() -> createPartitionedRegion(regionName2, localMaxMemory, 1));
    vm2.invoke(() -> createPartitionedRegion(regionName2, localMaxMemory, 1));
    vm3.invoke(() -> createPartitionedRegion(regionName2, localMaxMemory, 1));

    // validation of PRID metadata
    vm0.invoke(() -> validatePRIDCreation(regionName1, regionName2));
    vm1.invoke(() -> validatePRIDCreation(regionName1, regionName2));
    vm2.invoke(() -> validatePRIDCreation(regionName1, regionName2));
    vm3.invoke(() -> validatePRIDCreation(regionName2));
  }

  private void createPartitionedRegion(String regionName, int localMaxMemory, int redundancy) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    regionFactory.create(regionName);
  }

  private void validatePRIDCreation(final String... regionNames) {
    Cache cache = getCache();
    Region prRootRegion = cache.getRegion(PartitionedRegionHelper.PR_ROOT_REGION_NAME);
    assertThat(prRootRegion).isNotNull();

    List<Integer> prIdList = new ArrayList<>();
    for (String regionName : regionNames) {
      PartitionedRegion partitionedRegion = (PartitionedRegion) cache.getRegion(regionName);
      assertThat(partitionedRegion).isNotNull();

      PartitionRegionConfig prConfig =
          (PartitionRegionConfig) prRootRegion.get(partitionedRegion.getRegionIdentifier());
      assertThat(prConfig).isNotNull();

      prIdList.add(prConfig.getPRId());
    }

    // checking uniqueness of prId in allPartitionRegion
    SortedSet<Integer> prIdSet = new TreeSet<>(prIdList);
    assertThat(prIdSet).hasSameSizeAs(prIdList);

    int numberOfPartitionedRegions = regionNames.length;

    // prId generated should be between 0 to number of partition regions-1
    for (int prId : prIdSet) {
      assertThat(prId).isGreaterThanOrEqualTo(numberOfPartitionedRegions - 1);
    }

    // # of prId generated in allPartitionRegion should be equal to number of partition region
    assertThat(prIdSet).hasSize(numberOfPartitionedRegions);

    // # of prId generated in prIdToPR should be equal to number of partition region
    assertThat(PartitionedRegion.getPrIdToPR()).hasSize(numberOfPartitionedRegions);

    // checking uniqueness of prId in prIdToPR
    SortedSet<Integer> prIdPRSet = new TreeSet<>(PartitionedRegion.getPrIdToPR().keySet());
    assertThat(prIdPRSet).hasSameSizeAs(PartitionedRegion.getPrIdToPR().keySet());
  }
}
