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

package org.apache.geode.unsafe.internal.sun.misc;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

@Execution(CONCURRENT)
public class SignalTest {

  @Test
  public void wrapsSunSignal() {
    final Signal geodeSignal = new Signal("INT");
    final sun.misc.Signal sunSignal = new sun.misc.Signal("INT");

    assertThat(geodeSignal.signal).isEqualTo(sunSignal);
    assertThat(geodeSignal.getName()).isNotNull().isEqualTo(sunSignal.getName());
    assertThat(geodeSignal.getNumber()).isEqualTo(sunSignal.getNumber());
    assertThat(geodeSignal.hashCode()).isEqualTo(sunSignal.hashCode());
  }

  @Test
  public void unknownSignalThrowsException() {
    assertThatThrownBy(() -> new Signal("FAKE")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void handleWrapsSunHandler() {
    final sun.misc.SignalHandler sunHandler = mock(sun.misc.SignalHandler.class);
    final sun.misc.Signal sunSignal = new sun.misc.Signal("INT");
    final sun.misc.SignalHandler originalSunHandler = sun.misc.Signal.handle(sunSignal, sunHandler);
    try {
      final Signal geodeSignal = new Signal("INT");
      final SignalHandler geodeHandler = signal -> {
      };
      final SignalHandler originalGeodeHandler = Signal.handle(geodeSignal, geodeHandler);
      try {
        assertThat(originalGeodeHandler).isNotNull().isInstanceOf(Signal.SunSignalHandler.class);
        assertThat(((Signal.SunSignalHandler) originalGeodeHandler).signalHandler)
            .isSameAs(sunHandler);

        originalGeodeHandler.handle(geodeSignal);
        verify(sunHandler).handle(sunSignal);
        verifyNoMoreInteractions(sunHandler);
      } finally {
        assertThat(Signal.handle(geodeSignal, originalGeodeHandler)).isSameAs(geodeHandler);
      }
    } finally {
      assertThat(sun.misc.Signal.handle(sunSignal, originalSunHandler)).isSameAs(sunHandler);
    }
  }

  @Test
  public void handleNullThrowsException() {
    assertThatThrownBy(() -> Signal.handle(new Signal("INT"), null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Signal.handle(null, signal -> {
    }))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void raiseInvokesSunRaise() {
    final sun.misc.SignalHandler sunHandler = mock(sun.misc.SignalHandler.class);
    final sun.misc.Signal sunSignal = new sun.misc.Signal("INT");
    final sun.misc.SignalHandler originalSunHandler = sun.misc.Signal.handle(sunSignal, sunHandler);
    try {
      Signal.raise(new Signal("INT"));
      await().untilAsserted(() -> verify(sunHandler).handle(sunSignal));
      verifyNoMoreInteractions(sunHandler);
    } finally {
      sun.misc.Signal.handle(sunSignal, originalSunHandler);
    }
  }

  @Test
  public void raiseInvokesGeodeHandler() {
    final Signal geodeSignal = new Signal("INT");
    final SignalHandler geodeHandler = mock(SignalHandler.class);
    final SignalHandler originalGeodeHandler = Signal.handle(geodeSignal, geodeHandler);
    try {
      Signal.raise(geodeSignal);
      await().untilAsserted(() -> verify(geodeHandler).handle(geodeSignal));
      verifyNoMoreInteractions(geodeHandler);
    } finally {
      Signal.handle(geodeSignal, originalGeodeHandler);
    }
  }

  @Test
  public void raiseNullThrowsException() {
    assertThatThrownBy(() -> Signal.raise(null)).isInstanceOf(NullPointerException.class);
  }

}
