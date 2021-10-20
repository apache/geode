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
package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.RedisProperties.getIntegerSystemProperty;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;


public class RedisPropertiesTest {
  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenNeitherPrefixIsSet() {
    assertThat(getIntegerSystemProperty("prop.name", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToEmptyString() {
    System.setProperty("geode.prop.name0", "");
    assertThat(getIntegerSystemProperty("prop.name0", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToNonIntegerString() {
    System.setProperty("geode.prop.name0", "nonintegervalue");
    assertThat(getIntegerSystemProperty("prop.name0", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToIntegerOutOfRange() {
    System.setProperty("geode.prop.name1", "-5");
    assertThat(getIntegerSystemProperty("prop.name1", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSetValue_whenSetToIntegerInRange() {
    System.setProperty("geode.prop.name2", "10");
    assertThat(getIntegerSystemProperty("prop.name2", 5, 0)).isEqualTo(10);
  }

  @Test
  public void getIntegerSystemProperty_shouldDefaultToGeodePrefix_whenGemfireAlsoSet() {
    System.setProperty("geode.prop.name3", "15");
    System.setProperty("gemfire.prop.name3", "16");
    assertThat(getIntegerSystemProperty("prop.name3", 5, 0))
        .isEqualTo(15);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseGemfirePrefix_whenGeodePrefixOutOfRange() {
    System.setProperty("geode.prop.name4", "-1");
    System.setProperty("gemfire.prop.name4", "42");
    assertThat(getIntegerSystemProperty("prop.name4", 3, 0)).isEqualTo(42);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseDefault_whenPrefixesAreOutOfRange() {
    System.setProperty("geode.prop.name5", "-1");
    System.setProperty("gemfire.prop.name5", "-2");
    assertThat(getIntegerSystemProperty("prop.name5", 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseGemfirePrefix_whenGeodeNotSet() {
    System.setProperty("gemfire.prop.name6", "72");
    assertThat(getIntegerSystemProperty("prop.name6", 5, 0)).isEqualTo(72);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseSetValue_whenSetToMinimumValue() {
    System.setProperty("geode.prop.name7", "42");
    assertThat(getIntegerSystemProperty("prop.name7", 5, 42)).isEqualTo(42);
  }
}
