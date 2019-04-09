/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.configuration.validators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;

public class RegionConfigValidatorTest {

  private RegionConfigValidator validator;
  private RegionConfig config;

  @Before
  public void before() throws Exception {
    validator = new RegionConfigValidator();
    config = new RegionConfig();
  }

  @Test
  public void noChangesWhenTypeIsSet() {
    config.setName("regionName");
    config.setType(RegionType.REPLICATE);
    validator.validate(config);
    assertThat(config.getType()).isEqualTo("REPLICATE");
  }

  @Test
  public void invalidType() throws Exception {
    config.setName("regionName");
    config.setType("LOCAL");
    assertThatThrownBy(() -> validator.validate(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Type LOCAL is not supported in Management V2 API.");
  }

  @Test
  public void defaultsTypeToPartitioned() {
    config.setName("regionName");
    validator.validate(config);
    assertThat(config.getType()).isEqualTo("PARTITION");
  }

  @Test
  public void noName() {
    assertThatThrownBy(() -> validator.validate(config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining("Name of the region has to be specified");
  }

  @Test
  public void invalidName1() {
    config.setName("__test");
    assertThatThrownBy(() -> validator.validate(config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining("Region names may not begin with a double-underscore");
  }

  @Test
  public void invalidName2() {
    config.setName("a!&b");
    assertThatThrownBy(() -> validator.validate(config)).isInstanceOf(
        IllegalArgumentException.class)
        .hasMessageContaining(
            "Region names may only be alphanumeric and may contain hyphens or underscores");
  }
}
