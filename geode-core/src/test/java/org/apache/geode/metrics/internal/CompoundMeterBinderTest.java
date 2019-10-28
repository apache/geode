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
package org.apache.geode.metrics.internal;

import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.logging.log4j.Logger;
import org.junit.Test;

public class CompoundMeterBinderTest {
  @Test
  public void bindsAllBinders() {
    MeterRegistry registry = mock(MeterRegistry.class);

    Set<MeterBinder> meterBinders = setOf(7, MeterBinder.class);

    CompoundMeterBinder composedBinder = new CompoundMeterBinder(meterBinders);

    composedBinder.bindTo(registry);

    meterBinders.forEach(binder -> verify(binder).bindTo(same(registry)));
  }

  @Test
  public void logsWarning_ifBinderThrowsWhileBinding() {
    Logger logger = mock(Logger.class);

    RuntimeException thrownByBinder = new RuntimeException("thrown for test purposes");
    CloseableMeterBinder throwingBinder = mock(CloseableMeterBinder.class);
    doThrow(thrownByBinder).when(throwingBinder).bindTo(any());

    CompoundMeterBinder composedBinder = new CompoundMeterBinder(logger, singleton(throwingBinder));

    composedBinder.bindTo(mock(MeterRegistry.class));

    verify(logger).warn(any(String.class), same(thrownByBinder));
  }

  @Test
  public void closesAllCloseableBinders() {
    Set<MeterBinder> nonCloseableBinders = setOf(4, MeterBinder.class);
    Set<CloseableMeterBinder> closeableBinders = setOf(9, CloseableMeterBinder.class);

    Collection<MeterBinder> allBinders = new HashSet<>();
    allBinders.addAll(nonCloseableBinders);
    allBinders.addAll(closeableBinders);

    CompoundMeterBinder composedBinder = new CompoundMeterBinder(allBinders);

    composedBinder.close();

    closeableBinders.forEach(CompoundMeterBinderTest::verifyClosed);
  }

  @Test
  public void logsWarning_ifBinderThrowsWhileClosing() throws Exception {
    Logger logger = mock(Logger.class);

    RuntimeException thrownByBinder = new RuntimeException("thrown for test purposes");
    CloseableMeterBinder throwingBinder = mock(CloseableMeterBinder.class);
    doThrow(thrownByBinder).when(throwingBinder).close();

    CompoundMeterBinder composedBinder = new CompoundMeterBinder(logger, singleton(throwingBinder));

    composedBinder.close(); // throwingBinder will throw

    verify(logger).warn(any(String.class), same(thrownByBinder));

  }

  private static <T> Set<T> setOf(int count, Class<? extends T> type) {
    return IntStream.range(0, count)
        .mapToObj(i -> withSettings().name(type.getSimpleName() + i))
        .map(settings -> mock(type, settings))
        .collect(toSet());
  }

  private static void verifyClosed(CloseableMeterBinder binder) {
    try {
      verify(binder).close();
    } catch (Exception ignored) { // Ignored because mock binder will not throw
    }
  }
}
