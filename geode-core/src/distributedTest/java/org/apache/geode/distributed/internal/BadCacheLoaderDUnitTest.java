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
package org.apache.geode.distributed.internal;

import java.io.NotSerializableException;
import java.io.Serializable;
import java.util.Properties;

import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class BadCacheLoaderDUnitTest implements Serializable {
  public static final String TEST_REGION = "testRegion";
  public static final String TEST_KEY = "testKey";
  public static final String TEST_VALUE = "testValue";

  static class NotSerializableTestException extends RuntimeException {
    Object unserializableField = new Object();
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule(2);

  @Rule
  public CacheRule cacheRule = CacheRule.builder().build();

  /**
   * Ensure that a cache loader throwing an exception that is not serializable is handled
   * correctly
   */
  @Test
  public void testNonSerializableObjectReturnedByCacheLoader() throws Exception {
    final VM cacheLoaderVM = VM.getVM(0);
    final VM fetchingVM = VM.getVM(1);

    final Properties properties = new Properties();
    properties.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        NotSerializableTestException.class.getName());

    cacheLoaderVM.invoke("create a region with a bad cache loader",
        () -> createRegionWithBadCacheLoader(properties));

    Assertions.assertThatThrownBy(() -> fetchingVM.invoke("fetch something from the cache",
        () -> fetchValueCausingCacheLoad(properties)))
        .hasCauseInstanceOf(InternalGemFireException.class)
        .hasRootCauseInstanceOf(NotSerializableException.class)
        .hasRootCauseMessage("java.lang.Object");
  }

  private void fetchValueCausingCacheLoad(Properties properties) {
    final Cache cache = cacheRule.getOrCreateCache(properties);
    final Region<String, Object> testRegion =
        cache.<String, Object>createRegionFactory(RegionShortcut.PARTITION)
            .setCacheLoader(helper -> new Object())
            .create(TEST_REGION);
    testRegion.getAttributesMutator().setCacheLoader(helper -> "should not be invoked");
    testRegion.get(TEST_KEY);
  }

  private void createRegionWithBadCacheLoader(Properties properties) {
    final Cache cache = cacheRule.getOrCreateCache(properties);
    final Region<String, Object> testRegion =
        cache.<String, Object>createRegionFactory(RegionShortcut.PARTITION)
            .setCacheLoader(helper -> {
              throw new NotSerializableTestException();
            })
            .create(TEST_REGION);
    testRegion.put(TEST_KEY, TEST_VALUE);
    testRegion.destroy(TEST_KEY);
  }
}
