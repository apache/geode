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
package org.apache.geode.internal.logging.log4j;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LoggingTest;

/**
 * Unit tests for {@link FastLogger} which wraps and delegates to an actual Logger with
 * optimizations for isDebugEnabled and isTraceEnabled.
 */
@Category(LoggingTest.class)
public class FastLoggerTest {

  private static final String LOGGER_NAME = "LOGGER";
  private static final String MARKER_NAME = "MARKER";

  private FastLogger fastLogger;
  private ExtendedLogger mockedLogger;
  private Marker mockedMarker;

  @Before
  public void setUp() {
    MessageFactory messageFactory = new ParameterizedMessageFactory();
    mockedLogger = mock(ExtendedLogger.class);
    mockedMarker = mock(Marker.class);

    when(mockedLogger.getMessageFactory()).thenReturn(messageFactory);
    when(mockedLogger.getName()).thenReturn(LOGGER_NAME);
    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    when(mockedMarker.getName()).thenReturn(MARKER_NAME);

    fastLogger = new FastLogger(mockedLogger);

    FastLogger.setDelegating(true);

    clearInvocations(mockedLogger);

    assertThat(mockedLogger.getLevel()).isEqualTo(Level.INFO);
  }

  /**
   * FastLogger should return isDelegating after setDelegating
   */
  @Test
  public void returnIsDelegatingAfterSetDelegating() {
    assertThat(fastLogger.isDelegating()).isTrue();

    FastLogger.setDelegating(false);

    assertThat(fastLogger.isDelegating()).isFalse();
  }

  /**
   * FastLogger should delegate getLevel
   */
  @Test
  public void delegateGetLevel() {
    when(mockedLogger.getLevel()).thenReturn(Level.DEBUG);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.DEBUG);
  }

  /**
   * FastLogger should delegate isDebugEnabled when isDelegating
   */
  @Test
  public void delegateIsDebugEnabledWhenIsDelegating() {
    when(mockedLogger.getLevel()).thenReturn(Level.DEBUG);
    when(mockedLogger.isEnabled(eq(Level.DEBUG), isNull(), isNull())).thenReturn(true);
    when(mockedLogger.isEnabled(eq(Level.DEBUG), eq(mockedMarker), (Object) isNull(), isNull()))
        .thenReturn(true);

    assertThat(fastLogger.isDebugEnabled()).isTrue();
    assertThat(fastLogger.isDebugEnabled(mockedMarker)).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.DEBUG), isNull(), isNull());
    verify(mockedLogger).isEnabled(eq(Level.DEBUG), eq(mockedMarker), (Object) isNull(), isNull());
  }

  /**
   * FastLogger should delegate isTraceEnabled when isDelegating
   */
  @Test
  public void delegateIsTraceEnabledWhenIsDelegating() {
    when(mockedLogger.getLevel()).thenReturn(Level.TRACE);
    when(mockedLogger.isEnabled(eq(Level.TRACE), isNull(), (Object) isNull(), isNull()))
        .thenReturn(true);
    when(mockedLogger.isEnabled(eq(Level.TRACE), eq(mockedMarker), (Object) isNull(), isNull()))
        .thenReturn(true);

    assertThat(fastLogger.isTraceEnabled()).isTrue();
    assertThat(fastLogger.isTraceEnabled(mockedMarker)).isTrue();

    verify(mockedLogger).isEnabled(eq(Level.TRACE), isNull(), (Object) isNull(), isNull());
    verify(mockedLogger).isEnabled(eq(Level.TRACE), eq(mockedMarker), (Object) isNull(), isNull());
  }

  /**
   * FastLogger should not delegate isDebugEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsDebugEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);

    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.INFO);
    assertThat(fastLogger.isDebugEnabled()).isFalse();
    assertThat(fastLogger.isDebugEnabled(mockedMarker)).isFalse();

    verify(mockedLogger, never()).isEnabled(eq(Level.DEBUG), isNull(), isNull());
    verify(mockedLogger, never()).isEnabled(eq(Level.DEBUG), eq(mockedMarker), (Object) isNull(),
        isNull());
  }

  /**
   * FastLogger should not delegate isTraceEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsTraceEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);

    assertThat(fastLogger.getLevel()).isEqualTo(Level.INFO);

    assertThat(fastLogger.isTraceEnabled()).isFalse();
    verify(mockedLogger, never()).isEnabled(eq(Level.TRACE), isNull(), isNull());

    assertThat(fastLogger.isTraceEnabled(mockedMarker)).isFalse();
    verify(mockedLogger, never()).isEnabled(eq(Level.TRACE), eq(mockedMarker), (Object) isNull(),
        isNull());
  }

  /**
   * FastLogger should wrap delegate and return from getExtendedLogger
   */
  @Test
  public void wrapDelegateAndReturnFromGetExtendedLogger() {
    assertThat(fastLogger.getExtendedLogger()).isSameAs(mockedLogger);
  }

  /**
   * FastLogger should delegate getName
   */
  @Test
  public void delegateGetName() {
    assertThat(fastLogger.getName()).isEqualTo(LOGGER_NAME);

    verify(mockedLogger, never()).getName();
  }
}
