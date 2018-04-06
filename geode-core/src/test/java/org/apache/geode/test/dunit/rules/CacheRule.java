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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.test.dunit.DistributedTestUtils.getLocators;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that creates Cache instances in DistributedTest VMs without {@code CacheTestCase}.
 *
 * <p>
 * {@code CacheRule} follows the standard convention of using a {@code Builder} for configuration as
 * introduced in the JUnit {@code Timeout} rule.
 *
 * <p>
 * {@code CacheRule} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedTestRule distributedTestRule = new DistributedTestRule();
 *
 * {@literal @}Rule
 * public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();
 *
 * {@literal @}Test
 * public void everyVMShouldHaveACache() {
 *   assertThat(cacheRule.getCache()).isNotNull();
 *   for (VM vm : Host.getHost(0).getAllVMs()) {
 *     vm.invoke(() -> assertThat(cacheRule.getCache()).isNotNull());
 *   }
 * }
 * </pre>
 */
@SuppressWarnings({"serial", "unused"})
public class CacheRule extends DistributedExternalResource {

  private static volatile InternalCache cache;

  private final boolean createCacheInAll;
  private final boolean createCache;
  private final boolean disconnectAfter;
  private final List<VM> createCacheInVMs;
  private final Properties config;
  private final Properties systemProperties;

  public static Builder builder() {
    return new Builder();
  }

  public CacheRule() {
    this(new Builder());
  }

  CacheRule(final Builder builder) {
    createCacheInAll = builder.createCacheInAll;
    createCache = builder.createCache;
    disconnectAfter = builder.disconnectAfter;
    createCacheInVMs = builder.createCacheInVMs;
    config = builder.config;
    systemProperties = builder.systemProperties;
  }

  @Override
  protected void before() {
    if (createCacheInAll) {
      invoker().invokeInEveryVMAndController(() -> createCache(config, systemProperties));
    } else {
      if (createCache) {
        createCache(config, systemProperties);
      }
      for (VM vm : createCacheInVMs) {
        vm.invoke(() -> createCache(config, systemProperties));
      }
    }
  }

  @Override
  protected void after() {
    closeAndNullCache();
    invoker().invokeInEveryVMAndController(() -> closeAndNullCache());

    if (disconnectAfter) {
      Disconnect.disconnectAllFromDS();
    }
  }

  public InternalCache getCache() {
    return cache;
  }

  public InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  public void createCache() {
    cache = (InternalCache) new CacheFactory(config).create();
  }

  public void createCache(final Properties config) {
    cache = (InternalCache) new CacheFactory(config).create();
  }

  public void createCache(final Properties config, final Properties systemProperties) {
    System.getProperties().putAll(systemProperties);
    cache = (InternalCache) new CacheFactory(config).create();
  }

  public InternalCache getOrCreateCache() {
    if (cache == null) {
      createCache();
      assertThat(cache).isNotNull();
    }
    return cache;
  }

  private static void closeAndNullCache() {
    closeCache();
    nullCache();
  }

  private static void closeCache() {
    try {
      if (cache != null) {
        cache.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static void nullCache() {
    cache = null;
  }

  /**
   * Builds an instance of SharedCountersRule
   */
  public static class Builder {

    private boolean createCacheInAll;
    private boolean createCache;
    private boolean disconnectAfter;
    private List<VM> createCacheInVMs = new ArrayList<>();
    private Properties config = new Properties();
    private Properties systemProperties = new Properties();

    public Builder() {
      config.setProperty(LOCATORS, getLocators());
    }

    /**
     * Create Cache in every VM including controller and all DUnit VMs. Default is false.
     */
    public Builder createCacheInAll() {
      createCacheInAll = true;
      return this;
    }

    /**
     * Create Cache in specified VM. Default is none.
     */
    public Builder createCacheIn(final VM vm) {
      if (!createCacheInVMs.contains(vm)) {
        createCacheInVMs.add(vm);
      }
      return this;
    }

    /**
     * Create Cache in local JVM (controller). Default is false.
     */
    public Builder createCacheInLocal() {
      createCache = true;
      return this;
    }

    /**
     * Disconnect all VMs from DistributedSystem after each test. Cache is always closed regardless.
     * Default is false.
     */
    public Builder disconnectAfter() {
      disconnectAfter = true;
      return this;
    }

    public Builder replaceConfig(final Properties config) {
      this.config = config;
      return this;
    }

    public Builder addConfig(final String key, final String value) {
      config.put(key, value);
      return this;
    }

    public Builder addConfig(final Properties config) {
      this.config.putAll(config);
      return this;
    }

    public Builder addSystemProperty(final String key, final String value) {
      systemProperties.put(key, value);
      return this;
    }

    public Builder addSystemProperties(final Properties config) {
      systemProperties.putAll(config);
      return this;
    }

    public CacheRule build() {
      return new CacheRule(this);
    }
  }
}
