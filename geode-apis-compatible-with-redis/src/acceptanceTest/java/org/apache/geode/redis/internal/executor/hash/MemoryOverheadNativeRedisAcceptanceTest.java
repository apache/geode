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

import static org.junit.Assert.assertTrue;

import java.util.EnumMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.ClassRule;

import org.apache.geode.NativeRedisTestRule;

public class MemoryOverheadNativeRedisAcceptanceTest extends AbstractMemoryOverheadIntegrationTest {

  private static final Pattern FIND_USED_MEMORY = Pattern.compile("used_memory:(\\d+)");

  @ClassRule
  public static NativeRedisTestRule redis = new NativeRedisTestRule();

  @Override
  EnumMap<Measurement, Integer> expectedPerEntryOverhead() {
    EnumMap<Measurement, Integer> result = new EnumMap<>(Measurement.class);
    result.put(Measurement.STRING, 61);
    result.put(Measurement.SET, 223);
    result.put(Measurement.SET_ENTRY, 75);
    result.put(Measurement.HASH, 228);
    result.put(Measurement.HASH_ENTRY, 70);

    return result;
  }


  @Override
  public int getPort() {
    return redis.getPort();
  }

  @Override
  long getUsedMemory() {
    String memoryInfo = jedis.info("memory");
    Matcher matcher = FIND_USED_MEMORY.matcher(memoryInfo);
    assertTrue(matcher.find());
    String usedMemory = matcher.group(1);
    return Long.parseLong(usedMemory);
  }
}
