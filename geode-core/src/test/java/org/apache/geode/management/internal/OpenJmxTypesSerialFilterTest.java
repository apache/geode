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

public class OpenJmxTypesSerialFilterTest {

  private Consumer<String> infoLogger;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    infoLogger = uncheckedCast(mock(Consumer.class));
  }

  @Test
  public void propertyValueIsNullByDefault() {
    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);

    assertThat(propertyValue).isNull();
  }

  @Test
  public void supportsDedicatedSerialFilter_returnsTrue_atLeastJava9() {
    assumeThat(isJavaVersionAtLeast(JavaVersion.JAVA_9)).isTrue();

    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    boolean result = openJmxTypesSerialFilter.supportsDedicatedSerialFilter();

    assertThat(result).isTrue();
  }

  @Test
  public void supportsDedicatedSerialFilter_returnsFalse_atMostJava8() {
    assumeThat(isJavaVersionAtMost(JavaVersion.JAVA_1_8)).isTrue();

    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    boolean result = openJmxTypesSerialFilter.supportsDedicatedSerialFilter();

    assertThat(result).isFalse();
  }

  @Test
  public void setPropertyValue_setsValue_ifExistingValueIsNull() {
    String value = "value-of-property";
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    openJmxTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(value);
  }

  @Test
  public void setPropertyValue_setsValue_ifExistingValueIsEmpty() {
    System.setProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME, "");
    String value = "value-of-property";
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    openJmxTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(value);
  }

  @Test
  public void setPropertyValue_logsMessage_ifExistingValueIsEmpty() {
    System.setProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME, "");
    String value = "value-of-property";
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter =
        new OpenJmxTypesSerialFilter(infoLogger, () -> true);

    openJmxTypesSerialFilter.setPropertyValueUnlessExists(value);

    String expectedLogMessage = "System property " + OpenJmxTypesSerialFilter.PROPERTY_NAME +
        " is now configured with '" + value + "'.";
    verify(infoLogger).accept(expectedLogMessage);
  }

  @Test
  public void setPropertyValue_leavesExistingValue_ifExistingValueIsNotEmpty() {
    String existingValue = "existing-value-of-property";
    System.setProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME, existingValue);
    String value = "value-of-property";
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    openJmxTypesSerialFilter.setPropertyValueUnlessExists(value);

    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isEqualTo(existingValue);
  }

  @Test
  public void setPropertyValue_logsMessage_ifExistingValueIsNotEmpty() {
    String existingValue = "existing-value-of-property";
    System.setProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME, existingValue);
    String value = "value-of-property";
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter =
        new OpenJmxTypesSerialFilter(infoLogger, () -> true);

    openJmxTypesSerialFilter.setPropertyValueUnlessExists(value);

    String expectedLogMessage = "System property " + OpenJmxTypesSerialFilter.PROPERTY_NAME +
        " is already configured.";
    verify(infoLogger).accept(expectedLogMessage);
  }

  @Test
  public void createSerialFilterPattern_includesBoolean() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Boolean.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesByte() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Byte.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCharacter() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Character.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesShort() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Short.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesInteger() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Integer.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesLong() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Long.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesFloat() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Float.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesDouble() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(Double.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesString() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(String.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesBigInteger() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(BigInteger.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesBigDecimal() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(BigDecimal.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesObjectName() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(ObjectName.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCompositeData() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(CompositeData.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesTabularData() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(TabularData.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesSimpleType() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(SimpleType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesCompositeType() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(CompositeType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesTabularType() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(TabularType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesArrayType() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(ArrayType.class.getName());
  }

  @Test
  public void createSerialFilterPattern_includesMarshalledObject() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).contains(MarshalledObject.class.getName());
  }

  @Test
  public void createSerialFilterPattern_rejectsAllOtherTypes() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter = new OpenJmxTypesSerialFilter();

    String result = openJmxTypesSerialFilter.createSerialFilterPattern();

    assertThat(result).endsWith("!*");
  }

  @Test
  public void configureSerialFilterIfEmpty_setsPropertyValue_atLeastJava9() {
    OpenJmxTypesSerialFilter openJmxTypesSerialFilter =
        new OpenJmxTypesSerialFilter(infoLogger, () -> true);

    openJmxTypesSerialFilter.configureSerialFilterIfEmpty();

    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isNotEmpty();
  }

  @Test
  public void configureSerialFilterIfEmpty_setsPropertyValue_atMostJava8() {
    JmxRmiSerialFilter jmxRmiOpenTypesSerialFilter =
        new OpenJmxTypesSerialFilter(infoLogger, () -> false);

    jmxRmiOpenTypesSerialFilter.configureSerialFilterIfEmpty();

    String propertyValue = System.getProperty(OpenJmxTypesSerialFilter.PROPERTY_NAME);
    assertThat(propertyValue).isNull();
  }
}
