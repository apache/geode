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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.ENFORCE_UNIQUE_HOST;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Added specifically to test scenario of defect TRAC #47181.
 *
 * <p>
 * TRAC #47181: Combination of mcast-port=0, enforce-unique-host=true and redundancy-zone=x causes UnsupportedOperationException
 *
 * <p>
 * Extracted from {@link PartitionedRegionBucketCreationDistributionDUnitTest}.
 */

@Category(IntegrationTest.class)
public class EnforceUniqueHostInLonerIntegrationTest {

  private Cache cache;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    System.setProperty(GEMFIRE_PREFIX + "DISABLE_DISCONNECT_DS_ON_CACHE_CLOSE", "true");
    cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "").set(ENFORCE_UNIQUE_HOST, "true").set(REDUNDANCY_ZONE, "zone1").create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  /**
   * Added for defect #47181. Use below combination to reproduce the issue: mcast-port = 0 Locators
   * should be empty enforce-unique-host = true redundancy-zone = "zone"
   */
  @Test
  public void testEnforceUniqueHostForLonerDistributedSystem() throws Exception {
    AttributesFactory attr = new AttributesFactory();
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    PartitionAttributes prAttr = paf.create();
    attr.setPartitionAttributes(prAttr);
    RegionAttributes regionAttribs = attr.create();

    Region<String, Integer> region = cache.createRegion("PR1", regionAttribs);

    for (int i = 0; i < 113; i++) {
      region.put("Key_" + i, i);
    }

    assertThat(region.size()).isEqualTo(113);
  }

}
