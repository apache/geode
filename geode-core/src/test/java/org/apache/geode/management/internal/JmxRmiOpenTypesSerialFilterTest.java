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
package org.apache.geode.management.internal;

import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.rmi.MarshalledObject;
import java.util.function.Consumer;

import javax.management.ObjectName;
import javax.management.openmbean.ArrayType;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;

import org.apache.commons.lang3.JavaVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class JmxRmiOpenTypesSerialFilterTest {

  private Consumer<String> infoLogger;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    infoLogger = uncheckedCast(mock(Consumer.class));
  }

  @Test
  public void propertyValueIsNullByDefault() {
    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);

    assertThat(propertyValue).isNull();
  }

  @Test
  public void supportsDedicatedSerialFilter_returnsTrue_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    boolean result = jmxRmiOpenTypesSerialFilter.supportsDedicatedSerialFilter();

    assertThat(result).isTrue();
  }

  @Test
  public void supportsDedicatedSerialFilter_returnsFalse_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    boolean result = jmxRmiOpenTypesSerialFilter.supportsDedicatedSerialFilter();

    assertThat(result).isFalse();
  }

  @Test
  public void setPropertyValue_setsValue_ifExistingValueIsNull() {
    String value = "value-of-property";
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    jmxRmiOpenTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(value);
  }

  @Test
  public void setPropertyValue_setsValue_ifExistingValueIsEmpty() {
    System.setProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME, "");
    String value = "value-of-property";
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    jmxRmiOpenTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(value);
  }

  @Test
  public void setPropertyValue_logsMessage_ifExistingValueIsEmpty() {
    System.setProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME, "");
    String value = "value-of-property";
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter =
        new JmxRmiOpenTypesSerialFilter(infoLogger, () -> true);

    jmxRmiOpenTypesSerialFilter.setPropertyValueUnlessExists(value);

    String expectedLogMessage = "System property " + JmxRmiOpenTypesSerialFilter.PROPERTY_NAME +
        " is now configured with '" + value + "'.";
    verify(infoLogger).accept(expectedLogMessage);
  }

  @Test
  public void setPropertyValue_leavesExistingValue_ifExistingValueIsNotEmpty() {
    String existingValue = "existing-value-of-property";
    System.setProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME, existingValue);
    String value = "value-of-property";
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    jmxRmiOpenTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(existingValue);
  }

  @Test
  public void setPropertyValue_logsMessage_ifExistingValueIsNotEmpty() {
    String existingValue = "existing-value-of-property";
    System.setProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME, existingValue);
    String value = "value-of-property";
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter =
        new JmxRmiOpenTypesSerialFilter(infoLogger, () -> true);

    jmxRmiOpenTypesSerialFilter.setPropertyValueUnlessExists(value);

    String expectedLogMessage = "System property " + JmxRmiOpenTypesSerialFilter.PROPERTY_NAME +
        " is already configured.";
    verify(infoLogger).accept(expectedLogMessage);
  }

  @Test
  public void createSerialFilterPattern_includesBoolean() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Boolean.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesByte() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Byte.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCharacter() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Character.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesShort() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Short.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesInteger() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Integer.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesLong() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Long.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesFloat() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Float.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesDouble() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Double.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesString() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(String.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesBigInteger() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(BigInteger.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesBigDecimal() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(BigDecimal.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesObjectName() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(ObjectName.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCompositeData() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(CompositeData.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesTabularData() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(TabularData.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesSimpleType() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(SimpleType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCompositeType() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(CompositeType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesTabularType() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(TabularType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesArrayType() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(ArrayType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesMarshalledObject() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(MarshalledObject.class.getName());
  }

  @Test
  public void createSerialFilterPattern_rejectsAllOtherTypes() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter = new JmxRmiOpenTypesSerialFilter();

    String result = jmxRmiOpenTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).endsWith("!*");
  }

  @Test
  public void configureSerialFilterIfEmpty_setsPropertyValue_atLeastJava9() {
    JmxRmiOpenTypesSerialFilter jmxRmiOpenTypesSerialFilter =
        new JmxRmiOpenTypesSerialFilter(infoLogger, () -> true);

    jmxRmiOpenTypesSerialFilter.configureSerialFilter();

    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isNotEmpty();
  }

  @Test
  public void configureSerialFilterIfEmpty_setsPropertyValue_atMostJava8() {
    JmxRmiSerialFilter jmxRmiOpenTypesSerialFilter =
        new JmxRmiOpenTypesSerialFilter(infoLogger, () -> false);

    jmxRmiOpenTypesSerialFilter.configureSerialFilter();

    String propertyValue = System.getProperty(JmxRmiOpenTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isNull();
  }
}
