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
package org.apache.geode.cache30;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.Set;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.StateFlushOperation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of
 * {@link Scope#DISTRIBUTED_NO_ACK distributed no ACK}.
 *
 * @since GemFire 3.0
 */

public class DistributedNoAckRegionDUnitTest extends MultiVMRegionTestCase {

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_NO_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setConcurrencyChecksEnabled(false);
    return factory.create();
  }

  /**
   * Tests creating a distributed sub-region of a local scope region, which should fail.
   */
  @Test
  public void testDistSubregionOfLocalRegion() {
    // creating a distributed subregion of a LOCAL region is illegal.
    RegionFactory<Object, Object> factory = getCache().createRegionFactory();
    factory.setScope(Scope.LOCAL);
    Region<Object, Object> rootRegion = createRootRegion(factory);
    try {
      RegionFactory<Object, Object> factory2 = getCache().createRegionFactory();
      factory2.createSubregion(rootRegion, getUniqueName());
      fail("Should have thrown an IllegalStateException");
    } catch (IllegalStateException ignored) {
      // pass
    }
  }

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   *
   * @see RegionFactory#createSubregion
   */
  @Test
  public void testIncompatibleSubRegions() {
    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);


    // Scope.GLOBAL is illegal if there is any other cache in the
    // distributed system that has the same region with
    // Scope.DISTRIBUTED_NO_ACK.

    final String name = this.getUniqueName() + "-NO_ACK";
    vm0.invoke("Create NO ACK Region", () -> {
      try {
        RegionFactory<Object, Object> factory =
            getCache().createRegionFactory(getRegionAttributes());
        Region<Object, Object> region = createRegion(name, "INCOMPATIBLE_ROOT", factory);
        assertThat(
            getRootRegion("INCOMPATIBLE_ROOT").getAttributes().getScope().isDistributedNoAck())
                .isTrue();
        assertThat(region.getAttributes().getScope().isDistributedNoAck()).isTrue();
      } catch (CacheException ex) {
        fail("While creating NO ACK region", ex);
      }
    });

    vm1.invoke("Create GLOBAL Region", () -> {
      try {
        RegionFactory<Object, Object> factory =
            getCache().createRegionFactory(getRegionAttributes());
        factory.setScope(Scope.GLOBAL);
        assertThat(getRootRegion("INCOMPATIBLE_ROOT")).isNull();
        try {
          createRootRegion("INCOMPATIBLE_ROOT", factory);
          fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
          // pass...
        }

      } catch (CacheException ex) {
        fail("While creating GLOBAL Region", ex);
      }
    });

    vm1.invoke("Create ACK Region", () -> {

      try {
        RegionFactory<Object, Object> factory =
            getCache().createRegionFactory(getRegionAttributes());
        factory.setScope(Scope.DISTRIBUTED_ACK);
        assertThat(getRootRegion("INCOMPATIBLE_ROOT")).isNull();
        try {
          createRootRegion("INCOMPATIBLE_ROOT", factory);
          fail("Should have thrown an IllegalStateException");
        } catch (IllegalStateException ex) {
          // pass...
        }

      } catch (CacheException ex) {
        fail("While creating ACK Region", ex);
      }
    });
  }

  @Override
  protected void pauseIfNecessary(int ms) {
    Wait.pause(ms);
  }

  @Override
  protected void pauseIfNecessary() {
    Wait.pause();
  }

  @Override
  protected void flushIfNecessary(Region r) {
    DistributedRegion dr = (DistributedRegion) r;
    Set<InternalDistributedMember> targets = dr.getDistributionAdvisor().adviseCacheOp();
    StateFlushOperation.flushTo(targets, dr);
  }

  /**
   * The number of milliseconds to try repeating validation code in the event that AssertionError is
   * thrown. For DISTRIBUTED_NO_ACK scopes, a repeat timeout is used to account for the fact that a
   * previous operation may have not yet completed.
   */
  @Override
  protected long getRepeatTimeoutMs() {
    return 120 * 1000;
  }

}
