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
package org.apache.geode.internal.lang;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class SystemPropertyTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Test
  public void getIntegerPropertyPrefersGeodePrefix() {
    String testProperty = "testIntegerProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    String geodePrefixProperty = "geode." + testProperty;
    System.setProperty(geodePrefixProperty, "1");
    System.setProperty(gemfirePrefixProperty, "0");
    assertThat(SystemProperty.getProductIntegerProperty(testProperty).get()).isEqualTo(1);
    System.clearProperty(geodePrefixProperty);
    System.clearProperty(gemfirePrefixProperty);
  }

  @Test
  public void getIntegerPropertyReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testIntegerProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "1");
    assertThat(SystemProperty.getProductIntegerProperty(testProperty).get()).isEqualTo(1);
    System.clearProperty(gemfirePrefixProperty);
  }

  @Test
  public void getIntegerPropertyWithDefaultValue() {
    String testProperty = "testIntegerProperty";
    assertThat(SystemProperty.getProductIntegerProperty(testProperty, 1000)).isEqualTo(1000);
  }

  @Test
  public void getLongPropertyWithoutDefaultReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testLongProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "1");
    assertThat(SystemProperty.getProductLongProperty(testProperty).get()).isEqualTo(1);
    System.clearProperty(gemfirePrefixProperty);
  }

  @Test
  public void getLongPropertyWithDefaultValue() {
    String testProperty = "testIntegerProperty";
    assertThat(SystemProperty.getProductLongProperty(testProperty, 1000)).isEqualTo(1000);
  }

  @Test
  public void getIntegerPropertyReturnsEmptyOptionalIfPropertiesMissing() {
    String testProperty = "notSetProperty";
    assertThat(SystemProperty.getProductIntegerProperty(testProperty).isPresent()).isFalse();
  }

  @Test
  public void getBooleanPropertyReturnsEmptyOptionalIfProperiesMissing() {
    String testProperty = "notSetProperty";
    assertThat(SystemProperty.getProductBooleanProperty(testProperty).isPresent()).isFalse();
  }

  @Test
  public void getBooleanPropertyPrefersGeodePrefix() {
    String testProperty = "testBooleanProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    String geodePrefixProperty = "geode." + testProperty;
    System.setProperty(geodePrefixProperty, "true");
    System.setProperty(gemfirePrefixProperty, "false");
    assertThat(SystemProperty.getProductBooleanProperty(testProperty).get()).isTrue();
    System.clearProperty(geodePrefixProperty);
    System.clearProperty(gemfirePrefixProperty);
  }

  @Test
  public void getBooleanPropertyReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testBooleanProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "true");
    assertThat(SystemProperty.getProductBooleanProperty(testProperty).get()).isTrue();
    System.clearProperty(gemfirePrefixProperty);
  }
}
