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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;

/**
 * Test the replace method with an entry that has overflowed to disk.
 */
public class ReplaceWithOverflowJUnitTest {

  private static Cache cache;
  private Region<String, String> region;

  @BeforeClass
  public static void setUp() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("log-level", "info");
    cache = new CacheFactory(props).create();
  }

  @AfterClass
  public static void tearDown() {
    if (cache != null && !cache.isClosed()) {
      cache.close();
    }
  }

  @Before
  public void createRegion() {
    region = cache.<String, String>createRegionFactory()
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK))
        .setPartitionAttributes(
            new PartitionAttributesFactory<>().setTotalNumBuckets(1).create())
        .setDataPolicy(DataPolicy.PARTITION).create("ReplaceWithOverflowJUnitTest");
  }

  @After
  public void destroyRegion() {
    if (region != null) {
      region.destroyRegion();
    }
  }

  @Test
  public void testReplaceWithOverflow() {
    region.put("1", "1");
    region.put("2", "2");
    assertEquals(true, region.replace("1", "1", "one"));
  }

  @Test
  public void testReplaceWithNullValue() {
    region.create("3", null);
    assertEquals(false, region.replace("3", "foobar", "three"));
    assertEquals(true, region.replace("3", null, "three"));
    assertEquals(true, region.replace("3", "three", "3"));
    assertEquals(false, region.replace("3", null, "3"));
    region.invalidate("3");
    assertEquals(false, region.replace("3", "foobar", "three"));
    assertEquals(true, region.replace("3", null, "three"));
  }

}
