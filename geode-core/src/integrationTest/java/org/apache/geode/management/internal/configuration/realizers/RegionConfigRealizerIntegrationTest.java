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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionConfigRealizerIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  private RegionConfigRealizer realizer;
  private Region config;

  @Before
  public void setup() {
    config = new Region();
    realizer = new RegionConfigRealizer();
  }

  @Test
  public void sanityCheck() throws Exception {
    config.setName("test");
    config.setType(RegionType.REPLICATE);
    realizer.create(config, server.getCache());

    org.apache.geode.cache.Region<Object, Object> region = server.getCache().getRegion("test");
    assertThat(region).isNotNull();
    assertThat(region.getAttributes().getDataPolicy()).isEqualTo(DataPolicy.REPLICATE);
    assertThat(region.getAttributes().getScope()).isEqualTo(Scope.DISTRIBUTED_ACK);
  }

  @Test
  public void create2ndTime() throws Exception {
    config.setName("foo");
    config.setType(RegionType.REPLICATE);
    realizer.create(config, server.getCache());

    // the 2nd time with same name and type will throw an error
    assertThatThrownBy(() -> realizer.create(config, server.getCache()))
        .isInstanceOf(RegionExistsException.class);
  }

  @Test
  public void deleteRegion() {
    config.setName("foo");
    config.setType(RegionType.REPLICATE);
    realizer.create(config, server.getCache());

    org.apache.geode.cache.Region region = server.getCache().getRegion(config.getName());
    assertThat(region).isNotNull();

    realizer.delete(config, server.getCache());

    region = server.getCache().getRegion(config.getName());
    assertThat(region).isNull();
  }
}
