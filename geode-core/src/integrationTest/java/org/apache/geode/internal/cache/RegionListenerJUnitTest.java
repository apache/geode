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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;

public class RegionListenerJUnitTest {

  private GemFireCacheImpl cache;

  @After
  public void after() {
    if (cache != null) {
      cache.close();
    }
  }

  @Test
  public void test() {
    final AtomicBoolean afterCreateInvoked = new AtomicBoolean();
    final AtomicBoolean beforeDestroyedInvoked = new AtomicBoolean();
    RegionListener listener = new RegionListener() {

      @Override
      public RegionAttributes beforeCreate(Region parent, String regionName, RegionAttributes attrs,
          InternalRegionArguments internalRegionArgs) {
        AttributesFactory newAttrsFactory = new AttributesFactory(attrs);
        newAttrsFactory.setDataPolicy(DataPolicy.EMPTY);
        return newAttrsFactory.create();
      }

      @Override
      public void afterCreate(Region region) {
        afterCreateInvoked.set(true);
      }

      @Override
      public void beforeDestroyed(Region region) {
        beforeDestroyedInvoked.set(true);
      }
    };

    cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
    cache.addRegionListener(listener);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    assertEquals(DataPolicy.EMPTY, region.getAttributes().getDataPolicy());
    assertTrue(afterCreateInvoked.get());
    region.destroyRegion();
    assertTrue(beforeDestroyedInvoked.get());
  }

}
