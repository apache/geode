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
package org.apache.geode.management.internal.configuration.realizers;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionConfigRealizerIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  private RegionConfigRealizer realizer;
  private RegionConfig config;

  @Before
  public void setup() {
    config = new RegionConfig();
    realizer = new RegionConfigRealizer();
  }

  @Test
  public void sanityCheck() throws Exception {
    config.setName("test");
    config.setType(RegionType.REPLICATE);

    realizer.create(config, server.getCache());

    Region<Object, Object> region = server.getCache().getRegion("test");
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    assertThat(region.getAttributes().getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
  }
}
