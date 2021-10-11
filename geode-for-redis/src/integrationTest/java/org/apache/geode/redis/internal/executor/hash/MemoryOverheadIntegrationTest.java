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

import javax.management.MBeanServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.springframework.shell.converters.AvailableCommandsConverter;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.JvmSizeUtils;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.size.ObjectGraphSizer;
import org.apache.geode.redis.GeodeRedisServerRule;
import org.apache.geode.redis.internal.RegionProvider;

public class MemoryOverheadIntegrationTest extends AbstractMemoryOverheadIntegrationTest {
  protected static ObjectGraphSizer.ObjectFilter filter = new SkipBrokenClassesFilter();

  @Rule
  public GeodeRedisServerRule server = new GeodeRedisServerRule()
      .withProperty(ConfigurationProperties.LOG_LEVEL, "error");

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
    result.put(Measurement.STRING, 185);
    result.put(Measurement.SET, 270);
    result.put(Measurement.SET_ENTRY, 25);
    result.put(Measurement.HASH, 312);
    result.put(Measurement.HASH_ENTRY, 50);
    result.put(Measurement.SORTED_SET, 383);
    result.put(Measurement.SORTED_SET_ENTRY, 94);

    return result;
  }

  /**
   * Print out a histogram of one of our redis keys. This may help track down where memory
   * usage is coming from.
   */
  @After
  public void printHistogramOfOneRedisKey() throws IllegalAccessException {
    final PartitionedRegion dataRegion =
        (PartitionedRegion) CacheFactory.getAnyInstance()
            .getRegion(RegionProvider.DEFAULT_REDIS_DATA_REGION);
    final Object redisKey = dataRegion.keys().iterator().next();
    BucketRegion bucket = dataRegion.getBucketRegion(redisKey);
    RegionEntry entry = bucket.entries.getEntry(redisKey);
    System.out.println("----------------------------------------------------");
    System.out.println("Histogram of memory usage of first region entry");
    System.out.println("----------------------------------------------------");
    System.out.println(ObjectGraphSizer.histogram(entry, false));
  }

  @Override
  public int getPort() {
    return server.getPort();
  }

  @Override
  long getUsedMemory() {
    try {
      return ObjectGraphSizer.size(server.getServer(), filter, false);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("Couldn't compute size of cache", e);
    }
  }

  private static class SkipBrokenClassesFilter implements ObjectGraphSizer.ObjectFilter {
    @Override
    public boolean accept(Object parent, Object object) {
      return !(object instanceof AvailableCommandsConverter)
          && !(object instanceof MBeanServer);
    }
  }
}
