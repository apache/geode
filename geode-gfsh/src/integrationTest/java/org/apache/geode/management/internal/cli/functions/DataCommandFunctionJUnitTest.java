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
package org.apache.geode.management.internal.cli.functions;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.domain.DataCommandResult;
import org.apache.geode.test.junit.categories.GfshTest;

/**
 * TODO: Add additional tests for all methods in DataCommandFunction.
 *
 */
@Category({GfshTest.class})
public class DataCommandFunctionJUnitTest {

  private static Cache cache;

  private static final String PARTITIONED_REGION = "part_region";

  public static class StringCheese {
    private String cheese;

    public StringCheese() {}

    public StringCheese(final String cheese) {
      this.cheese = cheese;
    }

    public void setCheese(final String cheese) {
      this.cheese = cheese;
    }

    @Override
    public String toString() {
      return cheese;
    }

    @Override
    public int hashCode() {
      return this.cheese.hashCode();
    }

    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other instanceof StringCheese) {
        return this.cheese.equals(((StringCheese) other).cheese);
      }
      return false;
    }
  }

  @BeforeClass
  public static void setUp() {
    cache = new CacheFactory().set(MCAST_PORT, "0").create();
    RegionFactory<Object, String> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
    Region<Object, String> region1 = factory.create(PARTITIONED_REGION);

    region1.put(new StringCheese("key_1"), "value_1");
    region1.put("key_2", "value_2");
  }

  @AfterClass
  public static void tearDown() {
    cache.close();
    cache = null;
  }

  /*
   * This test addresses GEODE-184
   */
  @Test
  public void testLocateKeyIsObject() {
    DataCommandFunction dataCmdFn = new DataCommandFunction();

    DataCommandResult result =
        dataCmdFn.locateEntry("{'cheese': 'key_1'}", StringCheese.class.getName(),
            String.class.getName(), PARTITIONED_REGION, false, (InternalCache) cache);

    assertNotNull(result);
    result.aggregate(null);
    List<DataCommandResult.KeyInfo> keyInfos = result.getLocateEntryLocations();
    assertEquals(1, keyInfos.size());
  }

  @Test
  public void testLocateKeyIsString() {
    DataCommandFunction dataCmdFn = new DataCommandFunction();

    DataCommandResult result = dataCmdFn.locateEntry("key_2", String.class.getName(),
        String.class.getName(), PARTITIONED_REGION, false, (InternalCache) cache);

    assertNotNull(result);
    result.aggregate(null);
    List<DataCommandResult.KeyInfo> keyInfos = result.getLocateEntryLocations();
    assertEquals(1, keyInfos.size());
  }
}
