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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class tests the functionality of a cache {@link Region region} that has a scope of
 * {@link Scope#DISTRIBUTED_ACK distributed ACK}.
 *
 * @since GemFire 3.0
 */
@Category(DistributedTest.class)
public class DistributedAckRegionDUnitTest extends MultiVMRegionTestCase {

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    factory.setEarlyAck(false);
    factory.setConcurrencyChecksEnabled(false);
    return factory.create();
  }

  public Properties getDistributedSystemProperties() {
    Properties p = super.getDistributedSystemProperties();
    p.put(STATISTIC_SAMPLING_ENABLED, "true");
    p.put(LOG_LEVEL, LogWriterUtils.getDUnitLogLevel());
    return p;
  }

  /**
   * Tests the compatibility of creating certain kinds of subregions of a local region.
   *
   * @see Region#createSubregion
   */
  @Test
  public void testIncompatibleSubregions() throws CacheException, InterruptedException {

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    // Scope.GLOBAL is illegal if there is any other cache in the
    // distributed system that has the same region with
    // Scope.DISTRIBUTED_ACK.

    final String name = this.getUniqueName() + "-ACK";
    vm0.invoke(new SerializableRunnable("Create ACK Region") {
      public void run() {
        try {
          createRegion(name, "INCOMPATIBLE_ROOT", getRegionAttributes());

        } catch (CacheException ex) {
          Assert.fail("While creating ACK region", ex);
        }
      }
    });

    vm1.invoke(new SerializableRunnable("Create GLOBAL Region") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory(getRegionAttributes());
          factory.setScope(Scope.GLOBAL);
          try {
            createRegion(name, "INCOMPATIBLE_ROOT", factory.create());
            fail("Should have thrown an IllegalStateException");
          } catch (IllegalStateException ex) {
            // pass...
          }

        } catch (CacheException ex) {
          Assert.fail("While creating GLOBAL Region", ex);
        }
      }
    });
    vm1.invoke(new SerializableRunnable("Create NOACK Region") {
      public void run() {
        try {
          AttributesFactory factory = new AttributesFactory(getRegionAttributes());
          factory.setScope(Scope.DISTRIBUTED_NO_ACK);
          try {
            createRegion(name, "INCOMPATIBLE_ROOT", factory.create());
            fail("Should have thrown an IllegalStateException");
          } catch (IllegalStateException ex) {
            // pass...
          }

        } catch (CacheException ex) {
          Assert.fail("While creating NOACK Region", ex);
        }
      }
    });
  }
}
