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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;

@Category(DistributedTest.class)
public class DistributedAckOverflowRegionCCEDUnitTest extends
    DistributedAckRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEDUnitTest() {
    super();
  }

  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        5, EvictionAction.OVERFLOW_TO_DISK));
    return factory.create();
  }
  
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type
                                      + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setConcurrencyChecksEnabled(true);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
          5, EvictionAction.OVERFLOW_TO_DISK));
    }
    return factory.create();
  }

  @Override
  @Ignore
  @Test
  public void testClearWithConcurrentEvents() throws Exception {
    // TODO this test is disabled due to frequent failures.  See bug #
    // Remove this method from this class when the problem is fixed
//    super.testClearWithConcurrentEvents();
  }

  @Override
  @Ignore
  @Test
  public void testClearWithConcurrentEventsAsync() throws Exception {
    // TODO this test is disabled due to frequent failures.  See bug #
    // Remove this method from this class when the problem is fixed
//    super.testClearWithConcurrentEventsAsync();
  }
  
  
}
