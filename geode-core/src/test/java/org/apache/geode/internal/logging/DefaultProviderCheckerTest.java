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
package org.apache.geode.internal.logging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.internal.ClassPathLoader;

public class DefaultProviderCheckerTest {

  @Test
  public void isAvailableReturnsTrueIfAbleToLoadDefaultProviderClass() {
    DefaultProviderChecker checker = new DefaultProviderChecker(() -> Log4jContextFactory.class,
        (a) -> true, mock(Logger.class));

    boolean value = checker.isAvailable();

    assertThat(value).isTrue();
  }

  @Test
  public void isAvailableReturnsFalseIfUnableToLoadDefaultProviderClass() {
    DefaultProviderChecker checker = new DefaultProviderChecker(() -> Log4jContextFactory.class,
        (a) -> false, mock(Logger.class));

    boolean value = checker.isAvailable();

    assertThat(value).isFalse();
  }

  @Test
  public void isAvailableReturnsFalseIfNotUsingLog4jProvider() {
    DefaultProviderChecker checker = new DefaultProviderChecker(
        () -> mock(LoggerContextFactory.class).getClass(), (a) -> true, mock(Logger.class));

    boolean value = checker.isAvailable();

    assertThat(value).isFalse();
  }

  @Test
  public void logsUsingMessageIfUsingLog4jProvider() {
    Logger logger = mock(Logger.class);
    DefaultProviderChecker checker =
        new DefaultProviderChecker(() -> Log4jContextFactory.class, (a) -> true, logger);

    boolean value = checker.isAvailable();

    assertThat(value).isTrue();

    ArgumentCaptor<String> loggedMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).info(loggedMessage.capture());

    assertThat(loggedMessage.getValue())
        .isEqualTo("Log4j Core is available and using Log4jProvider.");
  }

  @Test
  public void logsNotUsingMessageIfNotUsingLog4jProvider() {
    Logger logger = mock(Logger.class);
    DefaultProviderChecker checker = new DefaultProviderChecker(
        () -> mock(LoggerContextFactory.class).getClass(), (a) -> true, logger);

    boolean value = checker.isAvailable();

    assertThat(value).isFalse();

    ArgumentCaptor<String> loggedMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).info(loggedMessage.capture());

    assertThat(loggedMessage.getValue())
        .isEqualTo("Log4j Core is available but not using Log4jProvider.");
  }

  @Test
  public void logsUnableToFindMessageIfClassNotFoundExceptionIsCaught() {
    Logger logger = mock(Logger.class);
    DefaultProviderChecker checker =
        new DefaultProviderChecker(() -> Log4jContextFactory.class, (a) -> false, logger);

    boolean value = checker.isAvailable();

    assertThat(value).isFalse();

    ArgumentCaptor<String> loggedMessage = ArgumentCaptor.forClass(String.class);
    verify(logger).info(loggedMessage.capture());

    assertThat(loggedMessage.getValue()).isEqualTo("Unable to find Log4j Core.");
  }

  @Test
  public void rethrowsIfIsClassLoadableFunctionThrowsRuntimeException() {
    RuntimeException exception = new RuntimeException("expected");
    DefaultProviderChecker checker =
        new DefaultProviderChecker(() -> Log4jContextFactory.class, (a) -> {
          throw exception;
        }, mock(Logger.class));

    Throwable thrown = catchThrowable(() -> checker.isAvailable());

    assertThat(thrown).isSameAs(exception);
  }

  @Test
  public void isClassLoadableReturnsTrueIfClassNameExists() {
    boolean value = DefaultProviderChecker.isClassLoadable(ClassPathLoader.class.getName());

    assertThat(value).isTrue();
  }

  @Test
  public void isClassLoadableReturnsFalseIfClassNameDoesNotExist() {
    boolean value = DefaultProviderChecker.isClassLoadable("Not a class");

    assertThat(value).isFalse();
  }

  @Test
  public void isClassLoadableThrowsNullPointerExceptionIfClassNameIsNull() {
    Throwable thrown = catchThrowable(() -> DefaultProviderChecker.isClassLoadable(null));

    assertThat(thrown).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void isClassLoadableReturnsFalseIfClassNameIsEmpty() {
    boolean value = DefaultProviderChecker.isClassLoadable("");

    assertThat(value).isFalse();
  }
}
