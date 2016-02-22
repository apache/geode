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

import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

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
    
    GemFireCacheImpl cache = (GemFireCacheImpl) new CacheFactory().set("mcast-port", "0").create();
    cache.addRegionListener(listener);
    Region region = cache.createRegionFactory(RegionShortcut.REPLICATE).create("region");
    assertEquals(DataPolicy.EMPTY, region.getAttributes().getDataPolicy());
    assertTrue(afterCreateInvoked.get());
  }

}
