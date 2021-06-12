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
 *
 */

package org.apache.geode.internal.cache;

import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;

public class ReplicateWithExpirationClearIntegrationTest {

  private Cache cache;
  private String regionName;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    regionName = testName.getMethodName() + "_Region";
    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void clearDoesNotLeaveEntryExpiryTaskInRegion() {
    RegionFactory<String, String> regionFactory = cache.createRegionFactory(REPLICATE);
    regionFactory.setEntryTimeToLive(new ExpirationAttributes(2000, DESTROY));
    Region<String, String> region = regionFactory.create(regionName);
    try {
      LocalRegion localRegion = (LocalRegion) region;
      region.put("key", "value");
      assertThat(localRegion.getEntryExpiryTasks()).hasSize(1);
      region.clear();
      assertThat(localRegion.getEntryExpiryTasks()).isEmpty();
    } finally {
      region.destroyRegion();
    }
  }
}
