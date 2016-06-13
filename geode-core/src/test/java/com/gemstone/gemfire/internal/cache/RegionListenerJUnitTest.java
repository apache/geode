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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class RegionListenerJUnitTest {

  @Test
  public void test() {
    final AtomicBoolean afterCreateInvoked = new AtomicBoolean(); 
    RegionListener listener = new RegionListener() {
      
      @Override
      public RegionAttributes beforeCreate(Region parent, String regionName,
          RegionAttributes attrs, InternalRegionArguments internalRegionArgs) {
        AttributesFactory newAttrsFactory = new AttributesFactory(attrs);
        newAttrsFactory.setDataPolicy(DataPolicy.EMPTY);
        return newAttrsFactory.create();
      }

      @Override
      public void afterCreate(Region region) {
        afterCreateInvoked.set(true);
      }
    };

    GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory().set(MCAST_PORT, "0").create();
    cache.addRegionListener(listener);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    assertEquals(DataPolicy.EMPTY, region.getAttributes().getDataPolicy());
    assertTrue(afterCreateInvoked.get());
  }

}
