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


import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;


public class DistributedAckOverflowRegionCCEDUnitTest extends DistributedAckRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEDUnitTest() {
    super();
  }

  @Override
  public <K, V> RegionAttributes<K, V> getRegionAttributes() {
    AttributesFactory<K, V> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    factory.setEvictionAttributes(
        EvictionAttributes.createLRUEntryAttributes(5, EvictionAction.OVERFLOW_TO_DISK));
    return factory.create();
  }

  @Override
  protected <K, V> RegionAttributes<K, V> getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type + " has been removed.");
    }
    AttributesFactory<K, V> factory = new AttributesFactory<>(ra);
    factory.setConcurrencyChecksEnabled(true);
    if (!ra.getDataPolicy().isEmpty()) {
      factory.setEvictionAttributes(
          EvictionAttributes.createLRUEntryAttributes(5, EvictionAction.OVERFLOW_TO_DISK));
    }
    return factory.create();
  }
}
