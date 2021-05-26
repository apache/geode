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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.GemFireCacheImpl;

/**
 * See bug 52093. Make sure that the contents of arrays are used to test equality on regions ops.
 */
public class RegionOperationsEqualityShouldUseArrayEqualsIntegrationTest {

  private GemFireCacheImpl createCache() {
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    GemFireCacheImpl result = (GemFireCacheImpl) new CacheFactory(props).create();
    return result;
  }

  private void closeCache(GemFireCacheImpl gfc) {
    gfc.close();
  }

  private void doOps(Region r) {
    {
      byte[] bytesValue = new byte[] {1, 2, 3, 4};
      r.put("bytesValue", bytesValue.clone());
      if (r.replace("bytesValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("bytesValue", bytesValue, "")) {
        fail("expected replace to happen");
      }
      r.put("bytesValue", bytesValue.clone());
      if (r.remove("bytesValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("bytesValue", bytesValue)) {
        fail("expected remove to happen");
      }
    }
    {
      boolean[] booleanValue = new boolean[] {true, false, true, false};
      r.put("booleanValue", booleanValue.clone());
      if (r.replace("booleanValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("booleanValue", booleanValue, "")) {
        fail("expected replace to happen");
      }
      r.put("booleanValue", booleanValue.clone());
      if (r.remove("booleanValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("booleanValue", booleanValue)) {
        fail("expected remove to happen");
      }
    }
    {
      short[] shortValue = new short[] {1, 2, 3, 4};
      r.put("shortValue", shortValue.clone());
      if (r.replace("shortValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("shortValue", shortValue, "")) {
        fail("expected replace to happen");
      }
      r.put("shortValue", shortValue.clone());
      if (r.remove("shortValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("shortValue", shortValue)) {
        fail("expected remove to happen");
      }
    }
    {
      char[] charValue = new char[] {1, 2, 3, 4};
      r.put("charValue", charValue.clone());
      if (r.replace("charValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("charValue", charValue, "")) {
        fail("expected replace to happen");
      }
      r.put("charValue", charValue.clone());
      if (r.remove("charValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("charValue", charValue)) {
        fail("expected remove to happen");
      }
    }
    {
      int[] intValue = new int[] {1, 2, 3, 4};
      r.put("intValue", intValue.clone());
      if (r.replace("intValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("intValue", intValue, "")) {
        fail("expected replace to happen");
      }
      r.put("intValue", intValue.clone());
      if (r.remove("intValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("intValue", intValue)) {
        fail("expected remove to happen");
      }
    }
    {
      long[] longValue = new long[] {1, 2, 3, 4};
      r.put("longValue", longValue.clone());
      if (r.replace("longValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("longValue", longValue, "")) {
        fail("expected replace to happen");
      }
      r.put("longValue", longValue.clone());
      if (r.remove("longValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("longValue", longValue)) {
        fail("expected remove to happen");
      }
    }
    {
      float[] floatValue = new float[] {1, 2, 3, 4};
      r.put("floatValue", floatValue.clone());
      if (r.replace("floatValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("floatValue", floatValue, "")) {
        fail("expected replace to happen");
      }
      r.put("floatValue", floatValue.clone());
      if (r.remove("floatValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("floatValue", floatValue)) {
        fail("expected remove to happen");
      }
    }
    {
      double[] doubleValue = new double[] {1, 2, 3, 4};
      r.put("doubleValue", doubleValue.clone());
      if (r.replace("doubleValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("doubleValue", doubleValue, "")) {
        fail("expected replace to happen");
      }
      r.put("doubleValue", doubleValue.clone());
      if (r.remove("doubleValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("doubleValue", doubleValue)) {
        fail("expected remove to happen");
      }
    }
    {
      Object[] oaValue = new Object[] {new byte[] {1, 2, 3, 4}, new short[] {1, 2, 3, 4},
          new int[] {1, 2, 3, 4}, "hello sweet world!"};
      r.put("oaValue", oaValue);
      Object[] deepCloneOaValue = new Object[] {new byte[] {1, 2, 3, 4}, new short[] {1, 2, 3, 4},
          new int[] {1, 2, 3, 4}, "hello sweet world!"};
      if (r.replace("oaValue", "", "")) {
        fail("expected replace to fail");
      }
      if (!r.replace("oaValue", deepCloneOaValue, "")) {
        fail("expected replace to happen");
      }
      r.put("oaValue", oaValue);
      if (r.remove("oaValue", "")) {
        fail("expected remove to fail");
      }
      if (!r.remove("oaValue", deepCloneOaValue)) {
        fail("expected remove to happen");
      }
    }
  }

  @Test
  public void testPartition() {
    GemFireCacheImpl gfc = createCache();
    try {
      Region r = gfc.createRegionFactory(RegionShortcut.PARTITION)
          .create("ArrayEqualsJUnitTestPartitionRegion");
      doOps(r);
    } finally {
      closeCache(gfc);
    }
  }

  @Test
  public void testLocal() {
    GemFireCacheImpl gfc = createCache();
    try {
      Region r =
          gfc.createRegionFactory(RegionShortcut.LOCAL).create("ArrayEqualsJUnitTestLocalRegion");
      doOps(r);
    } finally {
      closeCache(gfc);
    }
  }

}
