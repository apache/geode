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

import static org.apache.geode.test.dunit.Disconnect.disconnectAllFromDS;
import static org.apache.geode.test.dunit.VM.DEFAULT_VM_COUNT;
import static org.apache.geode.test.dunit.standalone.DUnitLauncher.getDistributedSystemProperties;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that creates Cache instances in DistributedTest VMs without {@code CacheTestCase}.
 *
 * <p>
 * {@code CacheRule} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public CacheRule cacheRule = new CacheRule();
 *
 * {@literal @}Before
 * public void setUp() {
 *   getVM(0).invoke(() -> cacheRule.createCache(new CacheFactory().setPdxDiskStore(myDiskStore))));
 * }
 *
 * {@literal @}Test
 * public void createRegionWithRegionFactory() {
 *   getVM(0).invoke(() -> {
 *     RegionFactory regionFactory = cacheRule.getCache().createRegionFactory();
 *     Region region = regionFactory.create("RegionName");
 *     assertThat(region).isNotNull();
 *   });
 * }
 * </pre>
 *
 * <p>
 * {@link CacheRule.Builder} can also be used to construct an instance with more options:
 *
 * <pre>
 * {@literal @}Rule
 * public DistributedRule distributedRule = new DistributedRule();
 *
 * {@literal @}Rule
 * public CacheRule cacheRule = CacheRule.builder().createCacheInAll().build();
 *
 * {@literal @}Test
 * public void controllerVmCreatedCache() {
 *   assertThat(cacheRule.getCache()).isNotNull();
 * }
 *
 * {@literal @}Test
 * public void remoteVmsCreatedCache() {
 *   for (VM vm : Host.getHost(0).getAllVMs()) {
 *     vm.invoke(() -> assertThat(cacheRule.getCache()).isNotNull());
 *   }
 * }
 * </pre>
 */
@SuppressWarnings("serial,unused")
public class CacheRule extends AbstractDistributedRule {

  private static volatile InternalCache cache;

  private final boolean createCacheInAll;
  private final boolean createCache;
  private final boolean disconnectAfter;
  private final boolean destroyRegions;
  private final boolean replaceConfig;
  private final List<VM> createCacheInVMs;
  private final Properties config;
  private final Properties systemProperties;

  /**
   * Use {@code Builder} for more options in constructing {@code CacheRule}.
   */
  public static Builder builder() {
    return new Builder();
  }

  public CacheRule() {
    this(new Builder());
  }

  public CacheRule(final int vmCount) {
    this(new Builder().vmCount(vmCount));
  }

  CacheRule(final Builder builder) {
    super(builder.vmCount);
    createCacheInAll = builder.createCacheInAll;
    createCache = builder.createCache;
    disconnectAfter = builder.disconnectAfter;
    destroyRegions = builder.destroyRegions;
    replaceConfig = builder.replaceConfig;
    createCacheInVMs = builder.createCacheInVMs;
    config = builder.config;
    systemProperties = builder.systemProperties;
  }

  @Override
  protected void before() {
    if (createCacheInAll) {
      invoker().invokeInEveryVMAndController(() -> createCache(config(), systemProperties));
    } else {
      if (createCache) {
        createCache(config(), systemProperties);
      }
      for (VM vm : createCacheInVMs) {
        vm.invoke(() -> createCache(config(), systemProperties));
      }
    }
  }

  @Override
  protected void after() {
    closeAndNullCache();
    invoker().invokeInEveryVMAndController(() -> closeAndNullCache());

    if (disconnectAfter) {
      disconnectAllFromDS();
    }
  }

  public InternalCache getCache() {
    return cache;
  }

  public InternalDistributedSystem getSystem() {
    return cache.getInternalDistributedSystem();
  }

  public void createCache() {
    cache = (InternalCache) new CacheFactory(config()).create();
  }

  public void createCache(final CacheFactory cacheFactory) {
    cache = (InternalCache) cacheFactory.create();
  }

  public void createCache(final Properties config) {
    cache = (InternalCache) new CacheFactory(config(config)).create();
  }

  public void createCache(final Properties config, final Properties systemProperties) {
    System.getProperties().putAll(systemProperties);
    cache = (InternalCache) new CacheFactory(config(config)).create();
  }

  public InternalCache getOrCreateCache() {
    if (cache == null || cache.isClosed()) {
      cache = null;
      createCache();
      assertThat(cache).isNotNull();
    }
    return cache;
  }

  public InternalCache getOrCreateCache(final CacheFactory cacheFactory) {
    if (cache == null || cache.isClosed()) {
      cache = null;
      createCache(cacheFactory);
      assertThat(cache).isNotNull();
    }
    return cache;
  }

  public InternalCache getOrCreateCache(final Properties config) {
    if (cache == null || cache.isClosed()) {
      cache = null;
      createCache(config);
      assertThat(cache).isNotNull();
    }
    return cache;
  }

  public InternalCache getOrCreateCache(final Properties config,
      final Properties systemProperties) {
    if (cache == null || cache.isClosed()) {
      cache = null;
      createCache(config, systemProperties);
      assertThat(cache).isNotNull();
    }
    return cache;
  }

  public void closeAndNullCache() {
    closeCache();
    nullCache();
  }

  private Properties config() {
    return config(new Properties());
  }

  private Properties config(final Properties config) {
    if (replaceConfig) {
      return config;
    }
    Properties allConfig = getDistributedSystemProperties();
    allConfig.putAll(this.config);
    allConfig.putAll(config);
    return allConfig;
  }

  private void closeCache() {
    try {
      if (cache != null) {
        if (destroyRegions) {
          destroyRegions(cache);
        }
        cache.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static void nullCache() {
    cache = null;
  }

  private static void destroyRegions(final Cache cache) {
    if (cache != null && !cache.isClosed()) {
      // try to destroy the root regions first so that we clean up any persistent files.
      for (Region<?, ?> root : cache.rootRegions()) {
        String regionFullPath = root == null ? null : root.getFullPath();
        // for colocated regions you can't locally destroy a partitioned region.
        if (root.isDestroyed() || root instanceof HARegion || root instanceof PartitionedRegion) {
          continue;
        }
        try {
          root.localDestroyRegion("CacheRule_tearDown");
        } catch (Exception ignore) {
        }
      }
    }
  }

  /**
   * Builds an instance of CacheRule.
   */
  public static class Builder {

    private final List<VM> createCacheInVMs = new ArrayList<>();
    private final Properties systemProperties = new Properties();

    private boolean createCacheInAll;
    private boolean createCache;
    private boolean disconnectAfter;
    private boolean destroyRegions;
    private boolean replaceConfig;
    private Properties config = new Properties();
    private int vmCount = DEFAULT_VM_COUNT;

    public Builder() {
      // nothing
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

    /**
     * Destroy all Regions before closing the Cache. This will cleanup the presence of each Region
     * in DiskStores, but this is not needed if the disk files are on a TemporaryFolder. Default is
     * false.
     */
    public Builder destroyRegions() {
      destroyRegions = true;
      return this;
    }

    public Builder replaceConfig(final Properties config) {
      this.config = config;
      replaceConfig = true;
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

    public Builder vmCount(final int vmCount) {
      this.vmCount = vmCount;
      return this;
    }

    public CacheRule build() {
      return new CacheRule(this);
    }
  }
}
