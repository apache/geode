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

import static org.apache.geode.distributed.ConfigurationProperties.ENFORCE_UNIQUE_HOST;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;

/**
 * Added specifically to test scenario of defect TRAC #47181.
 *
 * <p>
 * TRAC #47181: Combination of mcast-port=0, enforce-unique-host=true and redundancy-zone=x causes
 * UnsupportedOperationException
 *
 * <p>
 * Extracted from PartitionedRegionBucketCreationDistributionDUnitTest.
 */

public class EnforceUniqueHostForLonerIntegrationTest {

  private Cache cache;

  @After
  public void tearDown() {
    cache.close();
  }

  /**
   * Creation of cache should not throw UnsupportedOperationException
   */
  @Test
  public void enforceUniqueHostShouldBeUsableForLoner() throws Exception {
    cache = new CacheFactory().set(MCAST_PORT, "0").set(LOCATORS, "")
        .set(ENFORCE_UNIQUE_HOST, "true").set(REDUNDANCY_ZONE, "zone1").create();
    assertThat(cache.isClosed()).isFalse();
  }

}
