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

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class GlobalSerialFilterConfigurationTest {

  private SerializableObjectConfig config;
  private GlobalSerialFilter globalSerialFilter;
  private Logger logger;

  @Before
  public void setUp() {
    config = mock(SerializableObjectConfig.class);
    globalSerialFilter = mock(GlobalSerialFilter.class);
    logger = uncheckedCast(mock(Logger.class));
  }

  @Test
  public void logsInfo_whenOperationIsSuccessful() throws UnableToSetSerialFilterException {
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config, logger,
        (pattern, sanctionedClasses) -> globalSerialFilter);

    filterConfiguration.configure();

    verify(logger).info("Global serialization filter is now configured.");
    verify(logger, never()).warn(any(Object.class));
    verify(logger, never()).error(any(Object.class));
  }

  @Test
  public void rethrowsWhenIllegalStateExceptionIsThrownByApi()
      throws UnableToSetSerialFilterException {
    doThrow(new UnableToSetSerialFilterException(
        new IllegalStateException("Serial filter can only be set once")))
            .when(globalSerialFilter).setFilter();
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config,
        logger, (pattern, sanctionedClasses) -> globalSerialFilter);

    Throwable thrown = catchThrowable(() -> {
      filterConfiguration.configure();
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage("java.lang.IllegalStateException: Serial filter can only be set once")
        .hasRootCauseInstanceOf(IllegalStateException.class)
        .hasRootCauseMessage("Serial filter can only be set once");
  }

  @Test
  public void rethrowsWhenClassNotFoundExceptionIsThrownByApi()
      throws UnableToSetSerialFilterException {
    doThrow(new UnableToSetSerialFilterException(
        new ClassNotFoundException("sun.misc.ObjectInputFilter")))
            .when(globalSerialFilter).setFilter();
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config, logger,
        (pattern, sanctionedClasses) -> globalSerialFilter);

    Throwable thrown = catchThrowable(() -> {
      filterConfiguration.configure();
    });

    assertThat(thrown)
        .isInstanceOf(UnableToSetSerialFilterException.class)
        .hasMessage("java.lang.ClassNotFoundException: sun.misc.ObjectInputFilter")
        .hasRootCauseInstanceOf(ClassNotFoundException.class)
        .hasRootCauseMessage("sun.misc.ObjectInputFilter");
  }

  @Test
  public void setsValidateSerializableObjects() throws UnableToSetSerialFilterException {
    SerializableObjectConfig serializableObjectConfig = mock(SerializableObjectConfig.class);
    FilterConfiguration filterConfiguration =
        new GlobalSerialFilterConfiguration(serializableObjectConfig);

    filterConfiguration.configure();

    verify(serializableObjectConfig).setValidateSerializableObjects(true);
  }
}
