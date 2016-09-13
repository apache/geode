/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.cache30;

import static org.junit.Assert.*;

import org.junit.Ignore;
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
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This class tests the functionality of a cache {@link Region region}
 * that has a scope of {@link Scope#DISTRIBUTED_ACK distributed ACK}.
 *
 * @since GemFire 3.0
 */
@Category(DistributedTest.class)
@Ignore("TODO: test disabled because of GEODE-1568")
public class PreloadedRegionTestCase extends MultiVMRegionTestCase {

  /**
   * Returns region attributes for a <code>GLOBAL</code> region
   */
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.PRELOADED);
    return factory.create();
  } 

  /**
   * Tests that created entries are not propagated to other caches
   */
  @Test
  public void testDistributedCreate() throws Exception {
    final String rgnName = getUniqueName();

    SerializableRunnable create = new SerializableRunnable("testDistributedCreate: Create Region") {
      public void run() {
        try {
          createRegion(rgnName);
          getSystem().getLogWriter().info("testDistributedCreate: Created Region");
        }
        catch (CacheException e) {
          Assert.fail("While creating region", e);
        }
      }
    };

    SerializableRunnable newKey = new SerializableRunnable("testDistributedCreate: Create Key") {
      public void run() {
        try {
          Region root = getRootRegion("root");
          Region rgn = root.getSubregion(rgnName);
          rgn.create("key", null);
          getSystem().getLogWriter().info("testDistributedCReate: Created Key");
        }
        catch (CacheException e) {
          Assert.fail("While creating region", e);
        }
      }
    };

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    // Create empty region
    vm0.invoke(create);

    // Create empty version locally
    Region rgn = createRegion(rgnName);
    
    // Add a key in first cache
    vm0.invoke(newKey);
    
    // We should NOT see the update here.
    assertTrue(rgn.getEntry("key") == null);
  }

}
