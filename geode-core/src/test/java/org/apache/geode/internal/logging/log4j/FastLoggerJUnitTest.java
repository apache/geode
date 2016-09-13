/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.logging.log4j;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.*;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.message.ParameterizedMessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests the FastLogger class which wraps and delegates to an actual 
 * Logger with optimizations for isDebugEnabled and isTraceEnabled.
 */
@Category(UnitTest.class)
public class FastLoggerJUnitTest {

  private MessageFactory messageFactory;
  private ExtendedLogger mockedLogger;
  private Marker mockedMarker;
  
  @Before
  public void setUp() {
    this.messageFactory = new ParameterizedMessageFactory();
    this.mockedLogger = mock(ExtendedLogger.class);
    this.mockedMarker = mock(Marker.class);

    when(this.mockedLogger.getMessageFactory()).thenReturn(this.messageFactory);
    when(this.mockedMarker.getName()).thenReturn("MARKER");
  }
  
  /**
   * FastLogger should return isDelegating after setDelegating
   */
  @Test
  public void returnIsDelegatingAfterSetDelegating() {
    FastLogger.setDelegating(true);

    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
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
    when(this.mockedLogger.getLevel()).thenReturn(Level.DEBUG);
    
    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
    assertThat(fastLogger.getLevel(), is(Level.DEBUG));
    verify(this.mockedLogger, times(1)).getLevel();
  }
  
  /**
   * FastLogger should delegate isDebugEnabled when isDelegating
   */
  @Test
  public void delegateIsDebugEnabledWhenIsDelegating() {
    FastLogger.setDelegating(true);
    when(this.mockedLogger.getLevel()).thenReturn(Level.DEBUG);
    when(this.mockedLogger.isEnabled(eq(Level.DEBUG), isNull(Marker.class), isNull(String.class))).thenReturn(true);
    when(this.mockedLogger.isEnabled(eq(Level.DEBUG), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class))).thenReturn(true);

    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
    assertThat(fastLogger.isDebugEnabled(), is(true));
    assertThat(fastLogger.isDebugEnabled(this.mockedMarker), is(true));
    verify(this.mockedLogger, times(1)).isEnabled(eq(Level.DEBUG), any(Marker.class), isNull(String.class));
    verify(this.mockedLogger, times(1)).isEnabled(eq(Level.DEBUG), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class));
  }
  
  /**
   * FastLogger should delegate isTraceEnabled when isDelegating
   */
  @Test
  public void delegateIsTraceEnabledWhenIsDelegating() {
    FastLogger.setDelegating(true);
    when(this.mockedLogger.getLevel()).thenReturn(Level.TRACE);
    when(this.mockedLogger.isEnabled(eq(Level.TRACE), isNull(Marker.class), isNull(Object.class), isNull(Throwable.class))).thenReturn(true);
    when(this.mockedLogger.isEnabled(eq(Level.TRACE), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class))).thenReturn(true);

    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
    assertThat(fastLogger.isTraceEnabled(), is(true));
    assertThat(fastLogger.isTraceEnabled(this.mockedMarker), is(true));
    verify(this.mockedLogger, times(1)).isEnabled(eq(Level.TRACE), isNull(Marker.class), isNull(Object.class), isNull(Throwable.class));
    verify(this.mockedLogger, times(1)).isEnabled(eq(Level.TRACE), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class));
  }
  
  /**
   * FastLogger should not delegate isDebugEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsDebugEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);
    when(this.mockedLogger.getLevel()).thenReturn(Level.INFO);

    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
    assertThat(fastLogger.getLevel(), is(Level.INFO));
    assertThat(fastLogger.isDebugEnabled(), is(false));
    assertThat(fastLogger.isDebugEnabled(this.mockedMarker), is(false));
    verify(this.mockedLogger, times(0)).isEnabled(eq(Level.DEBUG), isNull(Marker.class), isNull(String.class));
    verify(this.mockedLogger, times(0)).isEnabled(eq(Level.DEBUG), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class));
  }
  
  /**
   * FastLogger should not delegate isTraceEnabled when not isDelegating
   */
  @Test
  public void notDelegateIsTraceEnabledWhenNotIsDelegating() {
    FastLogger.setDelegating(false);
    when(mockedLogger.getLevel()).thenReturn(Level.INFO);

    FastLogger fastLogger = new FastLogger(this.mockedLogger);
    
    assertThat(fastLogger.getLevel(), is(Level.INFO));
    assertThat(fastLogger.isTraceEnabled(), is(false));
    assertThat(fastLogger.isTraceEnabled(this.mockedMarker), is(false));
    verify(this.mockedLogger, times(0)).isEnabled(eq(Level.TRACE), isNull(Marker.class), isNull(String.class));
    verify(this.mockedLogger, times(0)).isEnabled(eq(Level.TRACE), eq(this.mockedMarker), isNull(Object.class), isNull(Throwable.class));
  }
  
  /**
   * FastLogger should wrap delegate and return from getExtendedLogger
   */
  @Test
  public void wrapDelegateAndReturnFromGetExtendedLogger() {
    FastLogger fastLogger = new FastLogger(this.mockedLogger);

    assertThat(fastLogger.getExtendedLogger(), is(sameInstance(this.mockedLogger)));
  }
  
  /**
   * FastLogger should delegate getName
   */
  @Test
  public void delegateGetName() {
    when(this.mockedLogger.getName()).thenReturn("name");

    FastLogger fastLogger = new FastLogger(this.mockedLogger);

    assertThat(fastLogger.getName(), is("name"));
    verify(this.mockedLogger, times(1)).getName();
  }
}
