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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
 * Unit tests the FastLogger class which wraps and delegates to an actual Logger with optimizations
 * for isDebugEnabled and isTraceEnabled.
 */
@Category(LoggingTest.class)
public class FastLoggerTest {

  private MessageFactory messageFactory;
  private ExtendedLogger mockedLogger;
  private Marker mockedMarker;

  @Before
  public void setUp() {
    messageFactory = new ParameterizedMessageFactory();
    mockedLogger = mock(ExtendedLogger.class);
    mockedMarker = mock(Marker.class);

    when(mockedLogger.getMessageFactory()).thenReturn(messageFactory);
    when(mockedMarker.getName()).thenReturn("MARKER");
  }

  /**
   * FastLogger should return isDelegating after setDelegating
   */
  @Test
  public void returnIsDelegatingAfterSetDelegating() {
    FastLogger.setDelegating(true);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.isDelegating(), is(true));

    FastLogger.setDelegating(false);

    assertThat(fastLogger.isDelegating(), is(false));
  }

  /**
   * FastLogger should delegate getLevel
   */
  @Test
  public void delegateGetLevel() {
    FastLogger.setDelegating(true);
    when(mockedLogger.getLevel()).thenReturn(Level.DEBUG);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.getLevel(), is(Level.DEBUG));
    verify(mockedLogger, times(1)).getLevel();
  }

  /**
   * FastLogger should delegate isDebugEnabled when isDelegating
   */
  @Test
  public void delegateIsDebugEnabledWhenIsDelegating() {
    FastLogger.setDelegating(true);
    when(mockedLogger.getLevel()).thenReturn(Level.DEBUG);
    when(mockedLogger.isEnabled(eq(Level.DEBUG), isNull(Marker.class), isNull(String.class)))
        .thenReturn(true);
    when(mockedLogger.isEnabled(eq(Level.DEBUG), eq(mockedMarker), isNull(Object.class),
        isNull(Throwable.class))).thenReturn(true);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.isDebugEnabled(), is(true));
    assertThat(fastLogger.isDebugEnabled(mockedMarker), is(true));
    verify(mockedLogger, times(1)).isEnabled(eq(Level.DEBUG), isNull(Marker.class),
        isNull(String.class));
    verify(mockedLogger, times(1)).isEnabled(eq(Level.DEBUG), eq(mockedMarker),
        isNull(Object.class), isNull(Throwable.class));
  }

  /**
   * FastLogger should delegate isTraceEnabled when isDelegating
   */
  @Test
  public void delegateIsTraceEnabledWhenIsDelegating() {
    FastLogger.setDelegating(true);
    when(mockedLogger.getLevel()).thenReturn(Level.TRACE);
    when(mockedLogger.isEnabled(eq(Level.TRACE), isNull(Marker.class), isNull(Object.class),
        isNull(Throwable.class))).thenReturn(true);
    when(mockedLogger.isEnabled(eq(Level.TRACE), eq(mockedMarker), isNull(Object.class),
        isNull(Throwable.class))).thenReturn(true);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.isTraceEnabled(), is(true));
    assertThat(fastLogger.isTraceEnabled(mockedMarker), is(true));
    verify(mockedLogger, times(1)).isEnabled(eq(Level.TRACE), isNull(Marker.class),
        isNull(Object.class), isNull(Throwable.class));
    verify(mockedLogger, times(1)).isEnabled(eq(Level.TRACE), eq(mockedMarker),
        isNull(Object.class), isNull(Throwable.class));
  }

  /**
   * FastLogger should not delegate isDebugEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsDebugEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);
    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.getLevel(), is(Level.INFO));
    assertThat(fastLogger.isDebugEnabled(), is(false));
    assertThat(fastLogger.isDebugEnabled(mockedMarker), is(false));
    verify(mockedLogger, times(0)).isEnabled(eq(Level.DEBUG), isNull(Marker.class),
        isNull(String.class));
    verify(mockedLogger, times(0)).isEnabled(eq(Level.DEBUG), eq(mockedMarker),
        isNull(Object.class), isNull(Throwable.class));
  }

  /**
   * FastLogger should not delegate isTraceEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsTraceEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);
    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.getLevel(), is(Level.INFO));
    assertThat(fastLogger.isTraceEnabled(), is(false));
    assertThat(fastLogger.isTraceEnabled(mockedMarker), is(false));
    verify(mockedLogger, times(0)).isEnabled(eq(Level.TRACE), isNull(Marker.class),
        isNull(String.class));
    verify(mockedLogger, times(0)).isEnabled(eq(Level.TRACE), eq(mockedMarker),
        isNull(Object.class), isNull(Throwable.class));
  }

  /**
   * FastLogger should wrap delegate and return from getExtendedLogger
   */
  @Test
  public void wrapDelegateAndReturnFromGetExtendedLogger() {
    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.getExtendedLogger(), is(sameInstance(mockedLogger)));
  }

  /**
   * FastLogger should delegate getName
   */
  @Test
  public void delegateGetName() {
    when(mockedLogger.getName()).thenReturn("name");

    FastLogger fastLogger = new FastLogger(mockedLogger);

    assertThat(fastLogger.getName(), is("name"));
    verify(mockedLogger, times(1)).getName();
  }
}
