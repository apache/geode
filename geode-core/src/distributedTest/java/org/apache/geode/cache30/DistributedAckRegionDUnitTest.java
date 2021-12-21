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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.LogWriterUtils;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of
 * {@link Scope#DISTRIBUTED_ACK distributed ACK}.
 *
 * @since GemFire 3.0
 */

public class DistributedAckRegionDUnitTest extends MultiVMRegionTestCase {

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setConcurrencyChecksEnabled(false);
    return factory.create();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(STATISTIC_SAMPLING_ENABLED, "true");
    p.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    return p;
  }

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   *
   * @see Region#create
   */
  @Test
  public void testIncompatibleSubregions() throws CacheException {

    // Scope.GLOBAL is illegal if there is any other cache in the
    // distributed system that has the same region with
    // Scope.DISTRIBUTED_ACK.

    final String name = getUniqueName() + "-ACK";
    vm0.invoke("Create ACK Region", () -> {
      createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());
    });

    vm1.invoke("Create GLOBAL Region", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
      factory.setScope(Scope.GLOBAL);
      try {
        createRegion(name, "INCOMPATIBLE_ROOT", factory);
        fail("Should have thrown an IllegalStateException");
      } catch (IllegalStateException ex) {
        // pass...
      }
    });

    vm1.invoke("Create NOACK Region", () -> {
      RegionFactory<Object, Object> factory = getCache().createRegionFactory(getRegionAttributes());
      factory.setScope(Scope.DISTRIBUTED_NO_ACK);
      try {
        createRegion(name, "INCOMPATIBLE_ROOT", factory);
        fail("Should have thrown an IllegalStateException");
      } catch (IllegalStateException ex) {
        // pass...
      }
    });
  }
}
