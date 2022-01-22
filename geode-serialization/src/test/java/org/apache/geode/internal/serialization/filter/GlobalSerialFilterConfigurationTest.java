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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

public class GlobalSerialFilterConfigurationTest {

  private SerializableObjectConfig config;
  private GlobalSerialFilter globalSerialFilter;
  private Consumer<String> infoLogger;
  private Consumer<String> warnLogger;
  private Consumer<String> errorLogger;

  @Before
  public void setUp() {
    config = mock(SerializableObjectConfig.class);
    globalSerialFilter = mock(GlobalSerialFilter.class);
    infoLogger = uncheckedCast(mock(Consumer.class));
    warnLogger = uncheckedCast(mock(Consumer.class));
    errorLogger = uncheckedCast(mock(Consumer.class));
  }

  @Test
  public void configureLogsInfo_whenOperationIsSuccessful() {
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config,
        infoLogger, warnLogger, errorLogger, (pattern, sanctionedClasses) -> globalSerialFilter);

    filterConfiguration.configure();

    verify(infoLogger).accept("Global serial filter is now configured.");
    verifyNoInteractions(warnLogger);
    verifyNoInteractions(errorLogger);
  }

  @Test
  public void configureLogsError_whenCauseOfUnsupportedOperationExceptionIsIllegalStateException() {
    doThrow(new UnsupportedOperationException(
        new IllegalStateException("Serial filter can only be set once")))
            .when(globalSerialFilter).setFilter();
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config,
        infoLogger, warnLogger, errorLogger, (pattern, sanctionedClasses) -> globalSerialFilter);

    filterConfiguration.configure();

    verifyNoInteractions(infoLogger);
    verify(warnLogger).accept("Global serial filter is already configured.");
    verifyNoInteractions(errorLogger);
  }

  @Test
  public void configureLogsError_whenCauseOfUnsupportedOperationExceptionIsClassNotFoundException() {
    doThrow(new UnsupportedOperationException(
        new ClassNotFoundException("sun.misc.ObjectInputFilter")))
            .when(globalSerialFilter).setFilter();
    FilterConfiguration filterConfiguration = new GlobalSerialFilterConfiguration(config,
        infoLogger, warnLogger, errorLogger, (pattern, sanctionedClasses) -> globalSerialFilter);

    filterConfiguration.configure();

    verifyNoInteractions(infoLogger);
    verifyNoInteractions(warnLogger);
    verify(errorLogger).accept(
        "Geode was unable to configure a global serialization filter because ObjectInputFilter not found.");
  }

  @Test
  public void configureSetsValidateSerializableObjects() {
    SerializableObjectConfig serializableObjectConfig = mock(SerializableObjectConfig.class);
    FilterConfiguration filterConfiguration =
        new GlobalSerialFilterConfiguration(serializableObjectConfig);

    filterConfiguration.configure();

    verify(serializableObjectConfig).setValidateSerializableObjects(true);
  }
}
