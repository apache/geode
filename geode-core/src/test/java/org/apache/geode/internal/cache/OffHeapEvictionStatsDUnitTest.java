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

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * Performs eviction stat dunit tests for off-heap regions.
 *
 * @since Geode 1.0
 */
@Category(DistributedTest.class)
public class OffHeapEvictionStatsDUnitTest extends EvictionStatsDUnitTest {

  @Override
  public final void preTearDownAssertions() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if (hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    Invoke.invokeInEveryVM(checkOrphans);
    checkOrphans.run();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties properties = super.getDistributedSystemProperties();
    properties.setProperty(OFF_HEAP_MEMORY_SIZE, "100m");

    return properties;
  }

  @Override
  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(getDistributedSystemProperties());
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
      LogWriterUtils.getLogWriter().info("cache= " + cache);
      LogWriterUtils.getLogWriter().info("cache closed= " + cache.isClosed());
      cache.getResourceManager().setEvictionOffHeapPercentage(20);
      LogWriterUtils.getLogWriter()
          .info("eviction= " + cache.getResourceManager().getEvictionOffHeapPercentage());
      LogWriterUtils.getLogWriter()
          .info("critical= " + cache.getResourceManager().getCriticalOffHeapPercentage());
    } catch (Exception e) {
      Assert.fail("Failed while creating the cache", e);
    }
  }

  @Override
  public boolean isOffHeapEnabled() {
    return true;
  }
}
