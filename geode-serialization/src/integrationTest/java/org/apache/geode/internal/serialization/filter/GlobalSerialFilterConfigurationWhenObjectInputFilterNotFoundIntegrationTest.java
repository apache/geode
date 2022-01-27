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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

public class GlobalSerialFilterConfigurationWhenObjectInputFilterNotFoundIntegrationTest {

  private final boolean supportsObjectInputFilter = false;

  private ReflectiveFacadeGlobalSerialFilterFactory globalSerialFilterFactory_throws;
  private SerializableObjectConfig config;
  private Logger logger;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void whenObjectInputFilter_classNotFound() {
    globalSerialFilterFactory_throws =
        new ReflectiveFacadeGlobalSerialFilterFactory(() -> {
          throw new UnsupportedOperationException(
              new ClassNotFoundException("sun.misc.ObjectInputFilter"));
        });
  }

  @Before
  public void setUpMocks() {
    config = mock(SerializableObjectConfig.class);
    logger = mock(Logger.class);
  }

  @Test
  public void doesNotThrow_whenEnableGlobalSerialFilterIsFalse_andObjectInputFilterClassNotFound() {
    System.clearProperty("geode.enableGlobalSerialFilter");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory();
    FilterConfiguration configuration = factory.create(config);

    assertThatCode(configuration::configure)
        .doesNotThrowAnyException();
  }

  @Test
  public void logsNothing_whenEnableGlobalSerialFilterIsFalse_andObjectInputFilterClassNotFound() {
    System.clearProperty("geode.enableGlobalSerialFilter");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory(() -> supportsObjectInputFilter);
    FilterConfiguration configuration = factory.create(config);

    configuration.configure();

    verifyNoInteractions(logger);
  }

  @Test
  public void doesNotConfigureOrThrow_whenEnableGlobalSerialFilterIsTrue_andObjectInputFilterClassNotFound() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    GlobalSerialFilterConfigurationFactory factory =
        new SystemPropertyGlobalSerialFilterConfigurationFactory(() -> supportsObjectInputFilter);
    FilterConfiguration configuration = factory.create(config);

    boolean result = configuration.configure();

    assertThat(result).isFalse();
  }

  @Test
  public void logsWarning_whenEnableGlobalSerialFilterIsTrue_andObjectInputFilterClassNotFound() {
    System.setProperty("geode.enableGlobalSerialFilter", "true");
    FilterConfiguration configuration = new GlobalSerialFilterConfiguration(
        config, logger, globalSerialFilterFactory_throws);

    configuration.configure();

    verify(logger, never()).info(any(Object.class));
    verify(logger).warn(
        "Unable to configure a global serialization filter because ObjectInputFilter not found. Please use Java release 8u121 or later that supports serialization filtering.");
    verify(logger, never()).error(any(Object.class));
  }
}
