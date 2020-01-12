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
package org.apache.geode.management.internal.configuration.mutators;

import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

public class RegionConfigMutatorIntegrationTest {

  @Rule
  public LocatorStarterRule locator = new LocatorStarterRule().withAutoStart();

  private RegionConfigManager mutator;
  private Region config;

  @Before
  public void before() throws Exception {
    config = new Region();
    mutator = new RegionConfigManager(null);
  }

  @Test
  public void sanity() throws Exception {
    config.setType(RegionType.REPLICATE);
    config.setName("test");
    CacheConfig cacheConfig =
        locator.getLocator().getConfigurationPersistenceService().getCacheConfig("cluster", true);

    mutator.add(config, cacheConfig);
    assertThat(find(cacheConfig.getRegions(), config.getId())).isNotNull();
  }
}
