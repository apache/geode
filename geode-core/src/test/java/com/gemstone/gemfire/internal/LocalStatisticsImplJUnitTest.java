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
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.LocalStatisticsImpl;
import com.gemstone.gemfire.internal.StatisticsImpl;
import com.gemstone.gemfire.internal.StatisticsManager;
import com.gemstone.gemfire.internal.StatisticsTypeImpl;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category(UnitTest.class)
public class LocalStatisticsImplJUnitTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private StatisticsImpl stats;

  @Before
  public void createStats() {
    final StatisticsTypeImpl type = mock(StatisticsTypeImpl.class);
    when(type.getIntStatCount()).thenReturn(5);
    when(type.getDoubleStatCount()).thenReturn(5);
    when(type.getLongStatCount()).thenReturn(5);
    final String textId = "";
    final long numbericId = 0;
    final long uniqueId = 0;
    final int osStatFlags = 0;
    final boolean atomicIncrements = false;
    final StatisticsManager system = mock(StatisticsManager.class);
    stats = new LocalStatisticsImpl(type, textId, numbericId, uniqueId, atomicIncrements, osStatFlags, system);
  }

  @Test
  public void invokeIntSuppliersShouldUpdateStats() {
    IntSupplier supplier1 = mock(IntSupplier.class);
    when(supplier1.getAsInt()).thenReturn(23);
    stats.setIntSupplier(4, supplier1);
    assertEquals(0, stats.invokeSuppliers());

    verify(supplier1).getAsInt();
    assertEquals(23, stats.getInt(4));
  }

  @Test
  public void invokeLongSuppliersShouldUpdateStats() {
    LongSupplier supplier1 = mock(LongSupplier.class);
    when(supplier1.getAsLong()).thenReturn(23L);
    stats.setLongSupplier(4, supplier1);
    assertEquals(0, stats.invokeSuppliers());

    verify(supplier1).getAsLong();
    assertEquals(23L, stats.getLong(4));
  }

  @Test
  public void invokeDoubleSuppliersShouldUpdateStats() {
    DoubleSupplier supplier1 = mock(DoubleSupplier.class);
    when(supplier1.getAsDouble()).thenReturn(23.3);
    stats.setDoubleSupplier(4, supplier1);
    assertEquals(0, stats.invokeSuppliers());

    verify(supplier1).getAsDouble();
    assertEquals(23.3, stats.getDouble(4), 0.1f);
  }

  @Test
  public void getSupplierCountShouldReturnCorrectCount() {
    IntSupplier supplier1 = mock(IntSupplier.class);
    stats.setIntSupplier(4, supplier1);
    assertEquals(1, stats.getSupplierCount());
  }

  @Test
  public void invokeSuppliersShouldCatchSupplierErrorsAndReturnCount() {
    IntSupplier supplier1 = mock(IntSupplier.class);
    when(supplier1.getAsInt()).thenThrow(NullPointerException.class);
    stats.setIntSupplier(4, supplier1);
    assertEquals(1, stats.invokeSuppliers());

    verify(supplier1).getAsInt();
  }

  @Test
  public void invokeSuppliersShouldLogErrorOnlyOnce() {
    final Logger originalLogger = StatisticsImpl.logger;
    try {
      final Logger logger = mock(Logger.class);
      StatisticsImpl.logger = logger;
      IntSupplier supplier1 = mock(IntSupplier.class);
      when(supplier1.getAsInt()).thenThrow(NullPointerException.class);
      stats.setIntSupplier(4, supplier1);
      assertEquals(1, stats.invokeSuppliers());
      verify(logger, times(1)).warn(anyString(), anyString(), anyInt(), isA(NullPointerException.class));
      assertEquals(1, stats.invokeSuppliers());
      //Make sure the logger isn't invoked again
      verify(logger, times(1)).warn(anyString(), anyString(), anyInt(), isA(NullPointerException.class));
    } finally {
      StatisticsImpl.logger = originalLogger;
    }
  }

  @Test
  public void badSupplierParamShouldThrowError() {
    IntSupplier supplier1 = mock(IntSupplier.class);
    when(supplier1.getAsInt()).thenReturn(23);
    thrown.expect(IllegalArgumentException.class);
    stats.setIntSupplier(23, supplier1);
  }
}
