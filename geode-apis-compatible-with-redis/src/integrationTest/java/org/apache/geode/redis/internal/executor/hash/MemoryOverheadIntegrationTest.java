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
package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumMap;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.shell.converters.AvailableCommandsConverter;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.JvmSizeUtils;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.size.ObjectGraphSizer;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.RegionProvider;

public class MemoryOverheadIntegrationTest extends AbstractMemoryOverheadIntegrationTest {
  protected static ObjectGraphSizer.ObjectFilter filter =
      (parent, object) -> !(object instanceof AvailableCommandsConverter);

  @Rule
  public GeodeRedisServerRule server = new GeodeRedisServerRule();

  @Before
  public void checkJVMPlatform() {
    // Fail
    assertThat(JvmSizeUtils.is64Bit())
        .withFailMessage("The expected values for these tests was computed for a 64 bit JVM")
        .isTrue();
  }

  @Override
  EnumMap<Measurement, Integer> expectedPerEntryOverhead() {
    EnumMap<Measurement, Integer> result = new EnumMap<>(Measurement.class);
    result.put(Measurement.STRING, 191);
    result.put(Measurement.SET, 382);
    result.put(Measurement.SET_ENTRY, 72);
    result.put(Measurement.HASH, 550);
    result.put(Measurement.HASH_ENTRY, 106);

    return result;
  }

  /**
   * Sample test that can show where the memory use of one of our redis entries is actually
   * coming from. It will print out a breakdown of each object reachable from the radish region
   * entry and how many bytes they use
   */
  @Test
  @Ignore
  public void showStringEntryHistogram() throws IllegalAccessException {

    // Set the empty key
    String response = jedis.set("", "");
    assertThat(response).isEqualTo("OK");

    // Extract the region entry from geode and show it's size

    final PartitionedRegion dataRegion =
        (PartitionedRegion) CacheFactory.getAnyInstance()
            .getRegion(RegionProvider.REDIS_DATA_REGION);
    final Object redisKey = dataRegion.keys().iterator().next();
    BucketRegion bucket = dataRegion.getBucketRegion(redisKey);
    RegionEntry entry = bucket.entries.getEntry(redisKey);
    System.out.println(ObjectGraphSizer.histogram(entry, false));
  }

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Override
  long getUsedMemory() {
    try {
      return ObjectGraphSizer.size(server.getServer(), filter, true);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Couldn't compute size of cache", e);
    }
  }
}
