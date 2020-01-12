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
package org.apache.geode.internal.process;

import static java.lang.System.lineSeparator;
import static org.apache.geode.internal.process.FileControllableProcess.fetchStatusWithValidation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;

/**
 * Unit tests for {@link FileControllableProcess}.
 */
public class ControllableProcessTest {

  @Test
  public void fetchStatusWithValidationThrowsIfJsonIsNull() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    when(state.toJson()).thenReturn(null);

    Throwable thrown = catchThrowable(() -> fetchStatusWithValidation(handler));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Null JSON for status is invalid");
  }

  @Test
  public void fetchStatusWithValidationThrowsIfJsonIsEmpty() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    when(state.toJson()).thenReturn("");

    Throwable thrown = catchThrowable(() -> fetchStatusWithValidation(handler));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty JSON for status is invalid");
  }

  @Test
  public void fetchStatusWithValidationThrowsIfJsonOnlyContainsSpaces() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    when(state.toJson()).thenReturn("  ");

    Throwable thrown = catchThrowable(() -> fetchStatusWithValidation(handler));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty JSON for status is invalid");
  }

  @Test
  public void fetchStatusWithValidationThrowsIfJsonOnlyContainsTabs() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    when(state.toJson()).thenReturn("\t\t");

    Throwable thrown = catchThrowable(() -> fetchStatusWithValidation(handler));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty JSON for status is invalid");
  }

  @Test
  public void fetchStatusWithValidationThrowsIfJsonOnlyContainsLineFeeds() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    when(state.toJson()).thenReturn(lineSeparator() + lineSeparator());

    Throwable thrown = catchThrowable(() -> fetchStatusWithValidation(handler));

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Empty JSON for status is invalid");
  }

  @Test
  public void fetchStatusWithValidationReturnsJsonIfItHasContent() {
    ControlNotificationHandler handler = mock(ControlNotificationHandler.class);
    ServiceState state = mock(ServiceState.class);
    when(handler.handleStatus()).thenReturn(state);
    String jsonContent = "json content";
    when(state.toJson()).thenReturn(jsonContent);

    String result = fetchStatusWithValidation(handler);

    assertThat(result).isEqualTo(jsonContent);
  }
}
