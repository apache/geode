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
package org.apache.geode.logging.internal.log4j.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.logging.internal.log4j.api.FastLogger;
import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link FastLogger} which wraps and delegates to an actual Logger with
 * optimizations for isDebugEnabled and isTraceEnabled.
 */
@Category(LoggingTest.class)
public class FastLoggerTest {

  private static final String LOGGER_NAME = "LOGGER";

  private FastLogger fastLogger;
  private ExtendedLogger mockedLogger;
  private Marker mockedMarker;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    MessageFactory messageFactory = new ParameterizedMessageFactory();
    mockedLogger = mock(ExtendedLogger.class);
    mockedMarker = mock(Marker.class);

    when(mockedLogger.getMessageFactory()).thenReturn(messageFactory);
    when(mockedLogger.getName()).thenReturn(LOGGER_NAME);
    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    fastLogger = new FastLogger(mockedLogger);

    FastLogger.setDelegating(true);

    clearInvocations(mockedLogger);

    assertThat(mockedLogger.getLevel()).isEqualTo(Level.INFO);
  }

  @Test
  public void isDelegatingIsTrueAfterSetDelegating() {
    assertThat(fastLogger.isDelegating()).isTrue();

    FastLogger.setDelegating(false);

    assertThat(fastLogger.isDelegating()).isFalse();
  }

  @Test
  public void delegatesGetLevel() {
    when(mockedLogger.getLevel()).thenReturn(Level.DEBUG);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.DEBUG);
  }

  @Test
  public void delegatesIsDebugEnabledWhenIsDelegating() {
    when(mockedLogger.isEnabled(eq(Level.DEBUG), nullable(Marker.class), nullable(String.class)))
        .thenReturn(true);

    assertThat(fastLogger.isDebugEnabled()).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.DEBUG), isNull(), isNull());
  }

  @Test
  public void delegatesIsDebugEnabledWithMarkerWhenIsDelegating() {
    when(mockedLogger.isEnabled(eq(Level.DEBUG), eq(mockedMarker), nullable(Object.class),
        nullable(Throwable.class)))
            .thenReturn(true);

    assertThat(fastLogger.isDebugEnabled(mockedMarker)).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.DEBUG), eq(mockedMarker), (Object) isNull(), isNull());
  }

  @Test
  public void delegatesIsTraceEnabledWhenIsDelegating() {
    when(mockedLogger.isEnabled(eq(Level.TRACE), nullable(Marker.class), nullable(Object.class),
        nullable(Throwable.class)))
            .thenReturn(true);

    assertThat(fastLogger.isTraceEnabled()).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.TRACE), isNull(), (Object) isNull(), isNull());
  }

  @Test
  public void delegatesIsTraceEnabledWithMarkerWhenIsDelegating() {
    when(mockedLogger.isEnabled(eq(Level.TRACE), eq(mockedMarker), nullable(Object.class),
        nullable(Throwable.class)))
            .thenReturn(true);

    assertThat(fastLogger.isTraceEnabled(mockedMarker)).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.TRACE), eq(mockedMarker), (Object) isNull(), isNull());
  }

  @Test
  public void doesNotDelegateIsDebugEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);

    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(fastLogger.isDebugEnabled()).isFalse();
    assertThat(fastLogger.isDebugEnabled(mockedMarker)).isFalse();

    verify(mockedLogger, never()).isEnabled(eq(Level.DEBUG), isNull(), isNull());
    verify(mockedLogger, never()).isEnabled(eq(Level.DEBUG), eq(mockedMarker), (Object) isNull(),
        isNull());
  }

  @Test
  public void doesNotDelegateIsTraceEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.INFO);

    assertThat(fastLogger.isTraceEnabled()).isFalse();
    verify(mockedLogger, never()).isEnabled(eq(Level.TRACE), isNull(), isNull());

    assertThat(fastLogger.isTraceEnabled(mockedMarker)).isFalse();
    verify(mockedLogger, never()).isEnabled(eq(Level.TRACE), eq(mockedMarker), (Object) isNull(),
        isNull());
  }

  @Test
  public void getExtendedLoggerReturnsDelegate() {
    assertThat(fastLogger.getExtendedLogger()).isSameAs(mockedLogger);
  }

  @Test
  public void delegatesGetName() {
    assertThat(fastLogger.getName()).isEqualTo(LOGGER_NAME);

    verify(mockedLogger, never()).getName();
  }
}
