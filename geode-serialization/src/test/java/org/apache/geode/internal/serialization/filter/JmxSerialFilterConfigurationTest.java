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
package org.apache.geode.internal.serialization.filter;

import static org.apache.geode.internal.serialization.filter.SerialFilterAssertions.assertThatSerialFilterIsNull;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.lang.reflect.InvocationTargetException;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class JmxSerialFilterConfigurationTest {

  private static final String SYSTEM_PROPERTY = "system.property.name";

  private String pattern;
  private Logger logger;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    pattern = "the-filter-pattern";
    logger = uncheckedCast(mock(Logger.class));
  }

  @After
  public void serialFilterIsNull() throws InvocationTargetException, IllegalAccessException {
    assertThatSerialFilterIsNull();
  }

  @Test
  public void propertyValue_isNullByDefault() {
    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isNull();
  }

  @Test
  public void setsPropertyValue() throws UnableToSetSerialFilterException {
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern);

    filterConfiguration.configure();

    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isNotEmpty();
  }

  @Test
  public void setsPropertyValue_ifExistingValueIsNull() throws UnableToSetSerialFilterException {
    System.clearProperty(SYSTEM_PROPERTY);
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern);

    filterConfiguration.configure();

    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isEqualTo(pattern);
  }

  @Test
  public void setsPropertyValue_ifExistingValueIsEmpty() throws UnableToSetSerialFilterException {
    System.setProperty(SYSTEM_PROPERTY, "");
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern);

    filterConfiguration.configure();

    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isEqualTo(pattern);
  }

  @Test
  public void setsPropertyValue_ifExistingValueIsBlank() throws UnableToSetSerialFilterException {
    System.setProperty(SYSTEM_PROPERTY, " ");
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern);

    filterConfiguration.configure();

    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isEqualTo(pattern);
  }

  @Test
  public void logsSuccess_ifExistingValueIsEmpty() throws UnableToSetSerialFilterException {
    System.setProperty(SYSTEM_PROPERTY, "");
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern, logger);

    filterConfiguration.configure();

    verify(logger)
        .info("System property '" + SYSTEM_PROPERTY + "' is now configured with '" +
            pattern + "'.");
  }

  @Test
  public void logsSuccess_ifExistingValueIsBlank() throws UnableToSetSerialFilterException {
    System.setProperty(SYSTEM_PROPERTY, " ");
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern, logger);

    filterConfiguration.configure();

    verify(logger)
        .info("System property '" + SYSTEM_PROPERTY + "' is now configured with '" +
            pattern + "'.");
  }

  @Test
  public void doesNotSetPropertyValue_ifExistingValueIsNotEmpty()
      throws UnableToSetSerialFilterException {
    String existingValue = "existing-value-of-property";
    System.setProperty(SYSTEM_PROPERTY, existingValue);
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern);

    filterConfiguration.configure();

    assertThat(System.getProperty(SYSTEM_PROPERTY))
        .as(SYSTEM_PROPERTY)
        .isEqualTo(existingValue);
  }

  @Test
  public void logsWarning_ifExistingPropertyValueIsNotEmpty()
      throws UnableToSetSerialFilterException {
    String existingValue = "existing-value-of-property";
    System.setProperty(SYSTEM_PROPERTY, existingValue);
    FilterConfiguration filterConfiguration =
        new JmxSerialFilterConfiguration(SYSTEM_PROPERTY, pattern, logger);

    filterConfiguration.configure();

    verify(logger)
        .info("System property '" + SYSTEM_PROPERTY + "' is already configured.");
  }
}
