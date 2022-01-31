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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class SystemPropertyGlobalSerialFilterConfigurationFactoryTest {

  private SerializableObjectConfig config;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() {
    config = mock(SerializableObjectConfig.class);
  }

  @Test
  public void createsNoOp_whenEnableGlobalSerialFilterIsFalse()
      throws UnableToSetSerialFilterException {
    System.clearProperty("geode.enableGlobalSerialFilter");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration configuration = factory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void createsEnabledGlobalSerialFilterConfiguration_whenEnableGlobalSerialFilterIsTrue() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration configuration = factory.create(config);

    // don't actually invoke configure because this is a unit test
    assertThat(configuration).isInstanceOf(GlobalSerialFilterConfiguration.class);
  }

  @Test
  public void createsNoOp_whenJdkSerialFilterExists() throws UnableToSetSerialFilterException {
    System.setProperty("jdk.serialFilter", "*");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration configuration = factory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void createsNoOp_whenJdkSerialFilterExists_andEnableGlobalSerialFilterIsTrue()
      throws UnableToSetSerialFilterException {
    System.setProperty("jdk.serialFilter", "*");
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    FilterConfiguration configuration = factory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void createsNoOp_whenEnableGlobalSerialFilterIsFalse_andJreDoesNotSupportObjectInputFilter()
      throws UnableToSetSerialFilterException {
    boolean supportsObjectInputFilter = false;
    GlobalSerialFilterConfigurationFactory configurationFactory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory(() -> supportsObjectInputFilter);

    FilterConfiguration configuration = configurationFactory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void createsNoOp_whenEnableGlobalSerialFilterIsTrue_andJreDoesNotSupportObjectInputFilter()
      throws UnableToSetSerialFilterException {
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    boolean supportsObjectInputFilter = false;
    GlobalSerialFilterConfigurationFactory configurationFactory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory(() -> supportsObjectInputFilter);

    FilterConfiguration configuration = configurationFactory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void doesNotThrow_whenEnableGlobalSerialFilterIsFalse_andJreDoesNotSupportObjectInputFilter() {
    System.clearProperty("geode.enableGlobalSerialFilter");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();

    assertThatCode(() -> {

      FilterConfiguration configuration = factory.create(config);

      configuration.configure();

    }).doesNotThrowAnyException();
  }
}
