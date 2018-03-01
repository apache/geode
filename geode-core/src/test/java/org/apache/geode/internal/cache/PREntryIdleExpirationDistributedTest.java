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

import static org.apache.geode.cache.ExpirationAction.DESTROY;
import static org.apache.geode.cache.RegionShortcut.PARTITION;

import org.junit.Before;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class PREntryIdleExpirationDistributedTest
    extends ReplicateEntryIdleExpirationDistributedTest {

  @Before
  public void setUpPREntryIdleExpirationTest() throws Exception {
    // make member1 the primary bucket for KEY
    member1.invoke(() -> {
      Region<String, String> region = cacheRule.getCache().getRegion(regionName);
      region.put(KEY, VALUE);
    });
  }

  @Override
  protected void createRegion() {
    RegionFactory<String, String> factory = cacheRule.getCache().createRegionFactory(PARTITION);
    factory.setPartitionAttributes(
        new PartitionAttributesFactory<String, String>().setRedundantCopies(2).create());
    factory.setEntryIdleTimeout(new ExpirationAttributes(1, DESTROY));
    factory.create(regionName);
  }
}
