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
package org.apache.geode.internal.cache.execute.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Test;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.internal.InternalEntity;

public class TimingFunctionInstrumentorTest {

  @Test
  public void instrument_returnsOriginalFunction_ifMeterRegistryNull() {
    FunctionInstrumentor functionInstrumentor = new TimingFunctionInstrumentor(() -> null);
    Function<?> originalFunction = mock(Function.class);

    Function<?> value = functionInstrumentor.instrument(originalFunction);

    assertThat(value).isSameAs(originalFunction);
  }

  @Test
  public void instrument_returnsOriginalFunction_ifFunctionImplementsInternalEntity() {
    FunctionInstrumentor functionInstrumentor =
        new TimingFunctionInstrumentor(SimpleMeterRegistry::new);
    Function<?> originalFunction = mock(Function.class, withSettings().extraInterfaces(
        InternalEntity.class));
    when(originalFunction.getId()).thenReturn("foo");

    Function<?> value = functionInstrumentor.instrument(originalFunction);

    assertThat(value).isSameAs(originalFunction);
  }

  @Test
  public void instrument_returnsTimingFunctionWithSameId() {
    FunctionInstrumentor functionInstrumentor =
        new TimingFunctionInstrumentor(SimpleMeterRegistry::new);
    Function<?> originalFunction = mock(Function.class);
    when(originalFunction.getId()).thenReturn("foo");

    Function<?> value = functionInstrumentor.instrument(originalFunction);

    assertThat(value).isInstanceOf(TimingFunction.class);
    assertThat(value.getId()).isEqualTo("foo");
  }

  @Test
  public void close_closesTimingFunction() {
    FunctionInstrumentor functionInstrumentor =
        new TimingFunctionInstrumentor(SimpleMeterRegistry::new);
    TimingFunction<?> function = mock(TimingFunction.class);

    functionInstrumentor.close(function);

    verify(function).close();
  }

  @Test
  public void close_doesNotCloseAutoCloseableFunction() {
    FunctionInstrumentor functionInstrumentor =
        new TimingFunctionInstrumentor(SimpleMeterRegistry::new);
    Function<?> function =
        mock(Function.class, withSettings().extraInterfaces(AutoCloseable.class));

    functionInstrumentor.close(function);

    verifyZeroInteractions(function);
  }
}
