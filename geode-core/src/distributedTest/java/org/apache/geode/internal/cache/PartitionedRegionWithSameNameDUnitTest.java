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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * Verifies creation of partitioned region and distributed region with same name.
 */

public class PartitionedRegionWithSameNameDUnitTest extends CacheTestCase {

  private String regionName;
  private String parentRegionName;
  private String childRegionName;

  private int localMaxMemory;
  private int redundancy;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    regionName = getUniqueName();
    parentRegionName = getUniqueName() + "_ParentRegion";
    childRegionName = getUniqueName() + "_ChildRegion";

    localMaxMemory = 200;
    redundancy = 0;
  }

  @Test
  public void testNameWithPartitionRegionFirstOnSameVM() {
    vm0.invoke(() -> givenPartitionedRegion(regionName));

    vm0.invoke(
        () -> validateCreateDistributedRegionThrows(regionName, RegionExistsException.class));
  }

  @Test
  public void testNameWithDistributedRegionFirstOnSameVM() {
    vm0.invoke(() -> givenDistributedRegion(regionName));

    vm0.invoke(
        () -> validateCreatePartitionedRegionThrows(regionName, RegionExistsException.class));
  }

  @Test
  public void testNameWithPartitionRegionFirstOnDifferentVM() {
    vm0.invoke(() -> givenPartitionedRegion(regionName));

    vm1.invoke(
        () -> validateCreateDistributedRegionThrows(regionName, IllegalStateException.class));
  }

  @Test
  public void testNameWithDistributedRegionFirstOnDifferentVM() {
    vm0.invoke(() -> givenDistributedRegion(regionName));

    vm1.invoke(
        () -> validateCreatePartitionedRegionThrows(regionName, IllegalStateException.class));
  }

  @Test
  public void testLocalRegionFirst() {
    vm0.invoke(() -> givenLocalRegion(regionName));

    vm1.invoke(() -> validateCreatePartitionedRegionSucceeds(regionName));
  }

  @Test
  public void testLocalRegionSecond() {
    vm0.invoke(() -> givenPartitionedRegion(regionName));

    vm1.invoke(() -> validateCreateLocalRegionSucceeds(regionName));
  }

  @Test
  public void testWithPartitionedRegionAsParentRegionAndDistributedSubRegion() {
    vm0.invoke(() -> givenPartitionedRegion(parentRegionName));

    vm0.invoke(() -> validateCreateDistributedSubRegionThrows(parentRegionName, childRegionName,
        UnsupportedOperationException.class));
  }

  @Test
  public void testWithPartitionedRegionAsParentRegionAndPartitionedSubRegion() {
    vm0.invoke(() -> givenPartitionedRegion(parentRegionName));

    vm0.invoke(() -> validateCreatePartitionedSubRegionThrows(parentRegionName, childRegionName,
        UnsupportedOperationException.class));
  }

  @Test
  public void testWithSubRegionPartitionedRegionFirst() {
    vm0.invoke(() -> givenDistributedRegion(parentRegionName));
    vm1.invoke(() -> givenDistributedRegion(parentRegionName));

    vm0.invoke(() -> givenPartitionedSubRegion(parentRegionName, childRegionName));

    vm1.invoke(() -> validateCreateDistributedSubRegionThrows(parentRegionName, childRegionName,
        IllegalStateException.class));
  }

  @Test
  public void testWithSubRegionDistributedRegionFirst() {
    vm0.invoke(() -> givenDistributedRegion(parentRegionName));
    vm1.invoke(() -> givenDistributedRegion(parentRegionName));
    vm2.invoke(() -> givenDistributedRegion(parentRegionName));
    vm3.invoke(() -> givenDistributedRegion(parentRegionName));

    vm0.invoke(() -> givenDistributedSubRegion(parentRegionName, childRegionName));

    vm1.invoke(() -> validateCreatePartitionedSubRegionThrows(parentRegionName, childRegionName,
        IllegalStateException.class));
    vm2.invoke(() -> validateCreatePartitionedSubRegionThrows(parentRegionName, childRegionName,
        IllegalStateException.class));
    vm3.invoke(() -> validateCreatePartitionedSubRegionThrows(parentRegionName, childRegionName,
        IllegalStateException.class));
  }

  private void givenDistributedRegion(final String regionName) {
    createDistributedRegion(regionName);
  }

  private void givenLocalRegion(final String regionName) {
    createLocalRegion(regionName);
  }

  private void givenPartitionedRegion(final String regionName) {
    createPartitionedRegion(regionName, localMaxMemory, redundancy);
  }

  private void givenDistributedSubRegion(final String parentRegionName,
      final String childRegionName) {
    Region parentRegion = getCache().getRegion(parentRegionName);
    assertThat(parentRegion).isNotNull();

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);

    Region childRegion = regionFactory.createSubregion(parentRegion, childRegionName);
    assertThat(childRegion).isNotNull();
  }

  private void givenPartitionedSubRegion(final String parentRegionName,
      final String childRegionName) {
    Region parentRegion = getCache().getRegion(parentRegionName);
    assertThat(parentRegion).isNotNull();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    Region childRegion = regionFactory.createSubregion(parentRegion, childRegionName);
    assertThat(childRegion).isNotNull();
  }

  private void createDistributedRegion(final String regionName) {
    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
  }

  private void createLocalRegion(final String regionName) {
    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.LOCAL);
    Region region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
  }

  private void createPartitionedRegion(final String regionName, final int localMaxMemory,
      final int redundancy) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    Region region = regionFactory.create(regionName);
    assertThat(region).isNotNull();
  }

  private void validateCreateDistributedRegionThrows(final String regionName,
      final Class<? extends Exception> exceptionClass) {
    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);
    assertThatThrownBy(() -> regionFactory.create(regionName)).isInstanceOf(exceptionClass);
  }

  private void validateCreateLocalRegionSucceeds(final String regionName) {
    assertThatCode(() -> createLocalRegion(regionName)).doesNotThrowAnyException();
  }

  private void validateCreatePartitionedRegionThrows(final String regionName,
      final Class<? extends Exception> exceptionClass) {
    assertThatThrownBy(() -> createPartitionedRegion(regionName, localMaxMemory, redundancy))
        .isInstanceOf(exceptionClass);
  }

  private void validateCreatePartitionedRegionSucceeds(final String regionName) {
    assertThatCode(() -> createPartitionedRegion(regionName, localMaxMemory, redundancy))
        .doesNotThrowAnyException();
  }

  private void validateCreateDistributedSubRegionThrows(final String parentRegionName,
      final String childRegionName, final Class<? extends Exception> exceptionClass) {
    Region parentRegion = getCache().getRegion(parentRegionName);
    assertThat(parentRegion).isNotNull();

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.REPLICATE);

    assertThatThrownBy(() -> regionFactory.createSubregion(parentRegion, childRegionName))
        .isInstanceOf(exceptionClass);
  }

  private void validateCreatePartitionedSubRegionThrows(final String parentRegionName,
      final String childRegionName, final Class<? extends Exception> exceptionClass) {
    Region parentRegion = getCache().getRegion(parentRegionName);
    assertThat(parentRegion).isNotNull();

    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setLocalMaxMemory(localMaxMemory);
    paf.setRedundantCopies(redundancy);

    RegionFactory regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(paf.create());

    assertThatThrownBy(() -> regionFactory.createSubregion(parentRegion, childRegionName))
        .isInstanceOf(exceptionClass);
  }
}
