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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Disconnect;
import org.apache.geode.test.dunit.VM;

/**
 * JUnit Rule that creates ClientCache instances in DistributedTest VMs without
 * {@code CacheTestCase}.
 *
 * <p>
 * {@code ClientCacheRule} follows the standard convention of using a {@code Builder} for
 * configuration as introduced in the JUnit {@code Timeout} rule.
 *
 * <p>
 * {@code ClientCacheRule} can be used in DistributedTests as a {@code Rule}:
 *
 * <pre>
 * {@literal @}ClassRule
 * public static DistributedTestRule distributedTestRule = new DistributedTestRule();
 *
 * {@literal @}Rule
 * public ClientCacheRule clientCacheRule = new ClientCacheRule();
 *
 * {@literal @}Test
 * public void hasClient() {
 *   vm0.invoke(() -> clientCacheRule.createClientCache());
 * }
 * </pre>
 */
@SuppressWarnings({"serial", "unused"})
public class ClientCacheRule extends AbstractDistributedTestRule {

  private static volatile InternalClientCache clientCache;

  private final boolean createClientCache;
  private final boolean disconnectAfter;
  private final List<VM> createClientCacheInVMs;
  private final Properties config;
  private final Properties systemProperties;

  public static Builder builder() {
    return new Builder();
  }

  public ClientCacheRule() {
    this(new Builder());
  }

  ClientCacheRule(final Builder builder) {
    createClientCache = builder.createClientCache;
    disconnectAfter = builder.disconnectAfter;
    createClientCacheInVMs = builder.createClientCacheInVMs;
    config = builder.config;
    systemProperties = builder.systemProperties;
  }

  @Override
  protected void before() {
    if (createClientCache) {
      createClientCache(config, systemProperties);
    }
    for (VM vm : createClientCacheInVMs) {
      vm.invoke(() -> createClientCache(config, systemProperties));
    }
  }

  @Override
  protected void after() {
    closeAndNullClientCache();
    invoker().invokeInEveryVMAndController(() -> closeAndNullClientCache());

    if (disconnectAfter) {
      Disconnect.disconnectAllFromDS();
    }
  }

  public InternalClientCache getClientCache() {
    return clientCache;
  }

  public InternalDistributedSystem getSystem() {
    return clientCache.getInternalDistributedSystem();
  }

  public void createClientCache() {
    clientCache = (InternalClientCache) new ClientCacheFactory(config).create();
  }

  public void createClientCache(final ClientCacheFactory ClientCacheFactory) {
    clientCache = (InternalClientCache) ClientCacheFactory.create();
  }

  public void createClientCache(final Properties config) {
    clientCache = (InternalClientCache) new ClientCacheFactory(config).create();
  }

  public void createClientCache(final Properties config, final Properties systemProperties) {
    System.getProperties().putAll(systemProperties);
    clientCache = (InternalClientCache) new ClientCacheFactory(config).create();
  }

  public InternalClientCache getOrCreateClientCache() {
    if (clientCache == null) {
      createClientCache();
      assertThat(clientCache).isNotNull();
    }
    return clientCache;
  }

  private static void closeAndNullClientCache() {
    closeClientCache();
    nullClientCache();
  }

  private static void closeClientCache() {
    try {
      if (clientCache != null) {
        clientCache.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
  }

  private static void nullClientCache() {
    clientCache = null;
  }

  /**
   * Builds an instance of ClientCacheRule.
   */
  public static class Builder {

    private boolean createClientCache;
    private boolean disconnectAfter;
    private List<VM> createClientCacheInVMs = new ArrayList<>();
    private Properties config = new Properties();
    private Properties systemProperties = new Properties();

    public Builder() {
      // nothing
    }

    /**
     * Create ClientCache in specified VM. Default is none.
     */
    public Builder createClientCacheIn(final VM vm) {
      if (!createClientCacheInVMs.contains(vm)) {
        createClientCacheInVMs.add(vm);
      }
      return this;
    }

    /**
     * Create Cache in local JVM (controller). Default is false.
     */
    public Builder createClientCacheInLocal() {
      createClientCache = true;
      return this;
    }

    /**
     * Disconnect from DistributedSystem in all VMs after each test. ClientCache is always closed
     * regardless. Default is false.
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

    public ClientCacheRule build() {
      return new ClientCacheRule(this);
    }
  }
}
