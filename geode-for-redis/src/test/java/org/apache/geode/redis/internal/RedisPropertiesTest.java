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

package org.apache.geode.redis.internal;

import static org.apache.geode.redis.internal.RedisProperties.getIntegerSystemProperty;
import static org.apache.geode.redis.internal.RedisProperties.getStringSystemProperty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class RedisPropertiesTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private final String basePropName = "prop-name";
  private final String propName = "geode.geode-for-redis-" + basePropName;
  private final String gemfirePropName = "gemfire.geode-for-redis-" + basePropName;
  private final String defaultValue = "default";

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenNeitherPrefixIsSet() {
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToEmptyString() {
    System.setProperty(propName, "");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToNonIntegerString() {
    System.setProperty(propName, "nonintegervalue");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSpecifiedDefault_whenSetToIntegerOutOfRange() {
    System.setProperty(propName, "-5");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldReturnSetValue_whenSetToIntegerInRange() {
    System.setProperty(propName, "10");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(10);
  }

  @Test
  public void getIntegerSystemProperty_shouldDefaultToGeodePrefix_whenGemfireAlsoSet() {
    System.setProperty(propName, "15");
    System.setProperty(gemfirePropName, "16");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(15);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseGemfirePrefix_whenGeodePrefixOutOfRange() {
    System.setProperty(propName, "-1");
    System.setProperty(gemfirePropName, "42");
    assertThat(getIntegerSystemProperty(propName, 3, 0)).isEqualTo(42);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseDefault_whenSetValuesAreLessThanMin() {
    System.setProperty(propName, "-1");
    System.setProperty(gemfirePropName, "-2");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseDefault_whenSetValuesAreMoreThanMax() {
    System.setProperty(propName, "100");
    System.setProperty(gemfirePropName, "200");
    assertThat(getIntegerSystemProperty(propName, 5, 0, 50)).isEqualTo(5);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseGemfirePrefix_whenGeodeNotSet() {
    System.setProperty(gemfirePropName, "72");
    assertThat(getIntegerSystemProperty(propName, 5, 0)).isEqualTo(72);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseSetValue_whenSetToMinimumValue() {
    System.setProperty(propName, "42");
    assertThat(getIntegerSystemProperty(propName, 5, 42)).isEqualTo(42);
  }

  @Test
  public void getIntegerSystemProperty_shouldUseSetValue_whenSetToMaximumValue() {
    System.setProperty(propName, "42");
    assertThat(getIntegerSystemProperty(propName, 5, 0, 42)).isEqualTo(42);
  }

  @Test
  public void getStringSystemProperty_shouldReturnSpecifiedDefault_whenNeitherAreSet() {
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getStringSystemProperty_shouldReturnSpecifiedDefault_whenSetToEmptyString_whenFalse() {
    System.setProperty(propName, "");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getStringSystemProperty_shouldReturnString_whenSet() {
    System.setProperty(propName, "not default");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo("not default");
  }

  @Test
  public void getStringSystemProperty_shouldDefaultToGeodePrefix_whenBothAreSet() {
    System.setProperty(propName, "String1");
    System.setProperty(gemfirePropName, "String2");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo("String1");
  }

  @Test
  public void getStringSystemProperty_shouldUseGemfirePrefix_whenGeodeNotSet() {
    System.setProperty(gemfirePropName, "String1");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo("String1");
  }

  @Test
  public void getStringSystemProperty_shouldUseDefault_whenBothSetToEmptyString() {
    System.setProperty(propName, "");
    System.setProperty(gemfirePropName, "");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo(defaultValue);
  }

  @Test
  public void getStringSystemProperty_shouldUseGemfirePrefix_whenGeodeSetToEmptyString() {
    System.setProperty(propName, "");
    System.setProperty(gemfirePropName, "String1");
    assertThat(getStringSystemProperty(propName, defaultValue)).isEqualTo("String1");
  }

  @Test
  public void getStringSystemProperty_throwsException_givenUnprefixedName() {
    assertThatThrownBy(
        () -> getStringSystemProperty("foo", ""))
            .hasMessageContaining("Property names must start with");
  }

  @Test
  public void getIntegerSystemProperty_throwsException_givenUnprefixedName() {
    assertThatThrownBy(
        () -> getIntegerSystemProperty("foo", 0, 0))
            .hasMessageContaining("Property names must start with");
  }
}
