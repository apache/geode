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
package org.apache.geode.internal;

import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;

/**
 * Cache.createRegionFactory should return a new instance with default values.
 *
 * <p>
 * Prior to the bug fix, Cache.createRegionFactory was internally reusing a modifiable instance of
 * RegionAttributes.
 *
 * <p>
 * TRAC #51616: Cache.getRegionAttributes returns modifiable RegionAttributes, with unintended side
 * effects
 *
 * <p>
 * Before the bug was fixed, the 2nd regionFactory.create threw this call stack:
 *
 * <pre>
 * java.lang.IllegalStateException: FixedPartitionAttributes "[FixedPartitionAttributes@[partitionName=one;isPrimary=true;numBuckets=111]]" can not be specified in PartitionAttributesFactory if colocated-with is specified.
 *     at com.gemstone.gemfire.internal.cache.PartitionAttributesImpl.validateWhenAllAttributesAreSet(PartitionAttributesImpl.java:569)
 *     at com.gemstone.gemfire.cache.AttributesFactory.validateAttributes(AttributesFactory.java:1515)
 *     at com.gemstone.gemfire.cache.AttributesFactory.create(AttributesFactory.java:1392)
 *     at com.gemstone.gemfire.cache.RegionFactory.create(RegionFactory.java:839)
 *     at PartitionAttributesTest.modifiedVcopsCreateCollocatedFixedRegions(PartitionAttributesTest.java:155)
 *     at PartitionAttributesTest.main(PartitionAttributesTest.java:47)
 * </pre>
 */
public class CreateRegionFactoryNewInstanceRegressionTest {

  private Cache cache;

  @Before
  public void setUp() {
    cache = new CacheFactory().set(LOCATORS, "").set(MCAST_PORT, "0").create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void createRegionFactoryShouldReturnNewInstanceWithDefaultValues() {
    PartitionAttributesFactory<Integer, String> paf = new PartitionAttributesFactory<>();
    paf.setTotalNumBuckets(111);
    paf.setRedundantCopies(0);
    paf.addFixedPartitionAttributes(createFixedPartition("one", true, 111));

    RegionFactory<Integer, String> regionFactory1 =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory1.setPartitionAttributes(paf.create());

    Region<Integer, String> region1 = regionFactory1.create("region1");

    PartitionAttributesFactory<String, Object> paf2 = new PartitionAttributesFactory<>();
    paf2.setColocatedWith(region1.getFullPath());
    paf2.setTotalNumBuckets(111);
    paf2.setRedundantCopies(0);

    RegionFactory<String, Object> regionFactory2 =
        cache.createRegionFactory(RegionShortcut.PARTITION);

    PartitionAttributes<String, Object> attrs2 = paf2.create();
    regionFactory2.setPartitionAttributes(attrs2);
    assertThatCode(() -> regionFactory2.create("region2")).doesNotThrowAnyException();
  }
}
