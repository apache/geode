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
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class JdbcAsyncWriterTest {

  private SqlHandler sqlHandler;
  private JdbcAsyncWriter writer;

  @Before
  public void setup() {
    sqlHandler = mock(SqlHandler.class);
    writer = new JdbcAsyncWriter(sqlHandler);
  }

  @Test
  public void throwsNullPointerExceptionIfGivenNullList() {
    assertThatThrownBy(() -> writer.processEvents(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void doesNothingIfEventListIsEmpty() {
    writer.processEvents(Collections.emptyList());

    verifyZeroInteractions(sqlHandler);
    assertThat(writer.getSuccessfulEvents()).isZero();
    assertThat(writer.getTotalEvents()).isZero();
  }

  @Test
  public void writesAProvidedEvent() {
    writer.processEvents(Collections.singletonList(createMockEvent()));

    verify(sqlHandler, times(1)).write(any(), any(), any(), any());
    assertThat(writer.getSuccessfulEvents()).isEqualTo(1);
    assertThat(writer.getTotalEvents()).isEqualTo(1);
  }

  @Test
  public void writesMultipleProvidedEvents() {
    List<AsyncEvent> events = new ArrayList<>();
    events.add(createMockEvent());
    events.add(createMockEvent());
    events.add(createMockEvent());

    writer.processEvents(events);

    verify(sqlHandler, times(3)).write(any(), any(), any(), any());
    assertThat(writer.getSuccessfulEvents()).isEqualTo(3);
    assertThat(writer.getTotalEvents()).isEqualTo(3);
  }

  private AsyncEvent createMockEvent() {
    AsyncEvent event = mock(AsyncEvent.class);
    when(event.getRegion()).thenReturn(mock(InternalRegion.class));
    return event;
  }
}
