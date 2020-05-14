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
package org.apache.geode.connectors.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.fake.Fakes;

public class JdbcWriterTest {

  private EntryEvent<Object, Object> entryEvent;
  private PdxInstance pdxInstance;
  private SqlHandler sqlHandler;
  private InternalRegion region;
  private SerializedCacheValue<Object> serializedNewValue;
  private Object key;

  private JdbcWriter<Object, Object> writer;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() {
    entryEvent = mock(EntryEvent.class);
    pdxInstance = mock(PdxInstance.class);
    sqlHandler = mock(SqlHandler.class);
    region = mock(InternalRegion.class);
    serializedNewValue = mock(SerializedCacheValue.class);
    InternalCache cache = Fakes.cache();
    key = "key";

    when(entryEvent.getRegion()).thenReturn(region);
    when(entryEvent.getKey()).thenReturn(key);
    when(entryEvent.getRegion().getRegionService()).thenReturn(cache);
    when(entryEvent.getSerializedNewValue()).thenReturn(serializedNewValue);
    when(entryEvent.getOperation()).thenReturn(Operation.CREATE);
    when(serializedNewValue.getDeserializedValue()).thenReturn(pdxInstance);

    writer = new JdbcWriter<>(sqlHandler, cache);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeUpdateWithPdxInstanceWritesToSqlHandler() throws Exception {
    writer.beforeUpdate(entryEvent);

    verify(sqlHandler, times(1)).write(eq(region), eq(Operation.CREATE), eq(key), eq(pdxInstance));
  }

  @Test
  public void beforeUpdateWithoutPdxInstanceWritesToSqlHandler() {
    when(serializedNewValue.getDeserializedValue()).thenReturn(new Object());

    assertThatThrownBy(() -> writer.beforeUpdate(entryEvent))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeCreateWithPdxInstanceWritesToSqlHandler() throws Exception {
    writer.beforeCreate(entryEvent);

    verify(sqlHandler, times(1)).write(eq(region), eq(Operation.CREATE), eq(key), eq(pdxInstance));
    assertThat(writer.getTotalEvents()).isEqualTo(1);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeCreateWithNewPdxInstanceWritesToSqlHandler() throws Exception {
    PdxInstance newPdxInstance = mock(PdxInstance.class);
    when(entryEvent.getNewValue()).thenReturn(newPdxInstance);
    when(entryEvent.getSerializedNewValue()).thenReturn(null);
    writer.beforeCreate(entryEvent);

    verify(sqlHandler, times(1)).write(eq(region), eq(Operation.CREATE), eq(key),
        eq(newPdxInstance));
    assertThat(writer.getTotalEvents()).isEqualTo(1);
  }

  @Test
  public void beforeCreateWithLoadEventDoesNothing() throws Exception {
    when(entryEvent.getOperation()).thenReturn(Operation.LOCAL_LOAD_CREATE);

    writer.beforeCreate(entryEvent);

    verify(sqlHandler, times(0)).write(any(), any(), any(), any());
    assertThat(writer.getTotalEvents()).isEqualTo(0);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeDestroyWithDestroyEventWritesToSqlHandler() throws Exception {
    when(entryEvent.getOperation()).thenReturn(Operation.DESTROY);
    when(entryEvent.getSerializedNewValue()).thenReturn(null);

    writer.beforeDestroy(entryEvent);

    verify(sqlHandler, times(1)).write(eq(region), eq(Operation.DESTROY), eq(key), eq(null));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeRegionDestroyDoesNotWriteToSqlHandler() {
    writer.beforeRegionDestroy(mock(RegionEvent.class));

    verifyZeroInteractions(sqlHandler);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void beforeRegionClearDoesNotWriteToSqlHandler() {
    writer.beforeRegionClear(mock(RegionEvent.class));

    verifyZeroInteractions(sqlHandler);
  }
}
