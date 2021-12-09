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

import static org.apache.geode.internal.lang.SystemProperty.getProductBooleanProperty;
import static org.apache.geode.internal.lang.SystemProperty.getProductIntegerProperty;
import static org.apache.geode.internal.lang.SystemProperty.getProductLongProperty;
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

    assertThat(getProductIntegerProperty(testProperty).get()).isEqualTo(1);
  }

  @Test
  public void getIntegerPropertyReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testIntegerProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "1");

    assertThat(getProductIntegerProperty(testProperty).get()).isEqualTo(1);
  }

  @Test
  public void getIntegerPropertyWithDefaultValue() {
    String testProperty = "testIntegerProperty";

    assertThat(getProductIntegerProperty(testProperty, 1000)).isEqualTo(1000);
  }

  @Test
  public void getLongPropertyWithoutDefaultReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testLongProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "1");

    assertThat(getProductLongProperty(testProperty).get()).isEqualTo(1);
  }

  @Test
  public void getLongPropertyWithDefaultValue() {
    String testProperty = "testIntegerProperty";

    assertThat(getProductLongProperty(testProperty, 1000)).isEqualTo(1000);
  }

  @Test
  public void getIntegerPropertyReturnsEmptyOptionalIfPropertiesMissing() {
    String testProperty = "notSetProperty";

    assertThat(getProductIntegerProperty(testProperty).isPresent()).isFalse();
  }

  @Test
  public void getBooleanPropertyReturnsEmptyOptionalIfProperiesMissing() {
    String testProperty = "notSetProperty";

    assertThat(getProductBooleanProperty(testProperty).isPresent()).isFalse();
  }

  @Test
  public void getBooleanPropertyPrefersGeodePrefix() {
    String testProperty = "testBooleanProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    String geodePrefixProperty = "geode." + testProperty;
    System.setProperty(geodePrefixProperty, "true");
    System.setProperty(gemfirePrefixProperty, "false");

    assertThat(getProductBooleanProperty(testProperty).get()).isTrue();
  }

  @Test
  public void getBooleanPropertyReturnsGemfirePrefixIfGeodeMissing() {
    String testProperty = "testBooleanProperty";
    String gemfirePrefixProperty = "gemfire." + testProperty;
    System.setProperty(gemfirePrefixProperty, "true");

    assertThat(getProductBooleanProperty(testProperty).get()).isTrue();
  }
}
