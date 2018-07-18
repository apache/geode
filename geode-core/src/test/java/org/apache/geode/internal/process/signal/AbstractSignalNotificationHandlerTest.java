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
package org.apache.geode.internal.process.signal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Set;

import org.junit.Test;

import org.apache.geode.internal.util.CollectionUtils;

/**
 * Unit tests for {@link AbstractSignalNotificationHandler}.
 *
 * @since GemFire 7.0
 */
public class AbstractSignalNotificationHandlerTest {

  @Test
  public void assertNotNullWithNonNullValueDoesNotThrow() {
    AbstractSignalNotificationHandler.assertNotNull(new Object(), "TEST");
  }

  @Test
  public void assertNotNullWithNullValueThrowsNullPointerException() {
    assertThatThrownBy(() -> AbstractSignalNotificationHandler.assertNotNull(null,
        "Expected %1$s message!", "test")).isInstanceOf(NullPointerException.class);
  }

  @Test
  public void assertStateWithValidStateDoesNotThrow() {
    AbstractSignalNotificationHandler.assertState(true, "TEST");
  }

  @Test
  public void assertStateWithInvalidStateThrowsIllegalStateException() {
    assertThatThrownBy(() -> AbstractSignalNotificationHandler.assertState(false,
        "Expected %1$s message!", "test")).isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void assertValidArgumentWithLegalArgumentDoesNotThrow() {
    AbstractSignalNotificationHandler.assertValidArgument(true, "TEST");
  }

  @Test
  public void assertValidArgumentWithIllegalArgumentThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> AbstractSignalNotificationHandler.assertValidArgument(false,
        "Expected %1$s message!", "test")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void registerListener() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    SignalListener mockListenerOne = mock(SignalListener.class, "SIGALL1");
    SignalListener mockListenerTwo = mock(SignalListener.class, "SIGALL2");

    assertThat(signalHandler.isListening(mockListenerOne)).isFalse();
    assertThat(signalHandler.isListening(mockListenerTwo)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
      assertThat(signalHandler.isListening(mockListenerOne, signal)).isFalse();
      assertThat(signalHandler.isListening(mockListenerTwo, signal)).isFalse();
    }

    assertThat(signalHandler.registerListener(mockListenerOne)).isTrue();
    assertThat(signalHandler.registerListener(mockListenerTwo)).isTrue();
    assertThat(signalHandler.registerListener(mockListenerTwo)).isFalse();
    assertThat(signalHandler.isListening(mockListenerOne)).isTrue();
    assertThat(signalHandler.isListening(mockListenerTwo)).isTrue();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isTrue();
      assertThat(signalHandler.isListening(mockListenerOne, signal)).isTrue();
      assertThat(signalHandler.isListening(mockListenerTwo, signal)).isTrue();
    }
  }

  @Test
  public void registerListenerWithNullSignalListenerThrowsNullPointerException() {
    assertThatThrownBy(() -> createSignalNotificationHandler().registerListener(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The SignalListener to register, listening for all signals cannot be null!");
  }

  @Test
  public void registerListenerWithSignal() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    SignalListener mockSigIntListener = mock(SignalListener.class, "SIGINT");
    SignalListener mockSigIntTermListener = mock(SignalListener.class, "SIGINT + SIGTERM");

    assertThat(signalHandler.isListening(mockSigIntListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigIntTermListener)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
      assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
      assertThat(signalHandler.isListening(mockSigIntTermListener, signal)).isFalse();
    }

    assertThat(signalHandler.registerListener(mockSigIntListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGTERM)).isTrue();
    assertThat(signalHandler.registerListener(mockSigIntTermListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigIntListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigIntTermListener)).isTrue();

    Set<Signal> expectedSignals = CollectionUtils.asSet(Signal.SIGINT, Signal.SIGTERM);

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isEqualTo(expectedSignals.contains(signal));
      switch (signal) {
        case SIGINT:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isTrue();
          assertThat(signalHandler.isListening(mockSigIntTermListener, signal)).isTrue();
          break;
        case SIGTERM:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigIntTermListener, signal)).isTrue();
          break;
        default:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigIntTermListener, signal)).isFalse();
      }
    }
  }

  @Test
  public void registerListenerWithNullSignalThrowsNullPointerException() {
    assertThatThrownBy(() -> createSignalNotificationHandler()
        .registerListener(mock(SignalListener.class, "SIGALL"), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("The signal to register the listener for cannot be null!");
  }

  @Test
  public void registerListenerWithSignalAndNullSignalListenerThrowsNullPointerException() {
    assertThatThrownBy(
        () -> createSignalNotificationHandler().registerListener(null, Signal.SIGQUIT))
            .isInstanceOf(NullPointerException.class)
            .hasMessage(signalListenerForSignalCannotBeNullErrorMessage(Signal.SIGQUIT));
  }

  @Test
  public void unregisterListener() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    SignalListener mockSignalListener = mock(SignalListener.class, "SIGALL");

    assertThat(signalHandler.isListening(mockSignalListener)).isFalse();
    assertThat(signalHandler.registerListener(mockSignalListener)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener)).isTrue();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isTrue();
    }

    assertThat(signalHandler.unregisterListener(mockSignalListener)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
    }

    assertThat(signalHandler.unregisterListener(mockSignalListener)).isFalse();
  }

  @Test
  public void unregisterListenerWithSignalListenerAndAllSignals() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    SignalListener mockSignalListener = mock(SignalListener.class, "SIGALL");

    assertThat(signalHandler.isListening(mockSignalListener)).isFalse();
    assertThat(signalHandler.registerListener(mockSignalListener)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener)).isTrue();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isTrue();
      assertThat(signalHandler.isListening(mockSignalListener, signal)).isTrue();
      assertThat(signalHandler.unregisterListener(mockSignalListener, signal)).isTrue();
      assertThat(signalHandler.isListening(mockSignalListener, signal)).isFalse();
      assertThat(signalHandler.hasListeners(signal)).isFalse();
    }

    assertThat(signalHandler.unregisterListener(mockSignalListener)).isFalse();
  }

  @Test
  public void unregisterListenerWithSignalListenerAndSigint() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();
    SignalListener mockSignalListener = mock(SignalListener.class, "SIGALL");

    assertThat(signalHandler.isListening(mockSignalListener)).isFalse();
    assertThat(signalHandler.registerListener(mockSignalListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener, Signal.SIGINT)).isTrue();

    for (Signal signal : Signal.values()) {
      if (!Signal.SIGINT.equals(signal)) {
        assertThat(signalHandler.hasListeners(signal)).isFalse();
        assertThat(signalHandler.isListening(mockSignalListener, signal)).isFalse();
      }
    }

    assertThat(signalHandler.isListening(mockSignalListener)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.unregisterListener(mockSignalListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.isListening(mockSignalListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSignalListener)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
    }

    assertThat(signalHandler.unregisterListener(mockSignalListener)).isFalse();
  }

  @Test
  public void unregisterListenerWithSignalListenerAndNullSignalThrowsNullPointerException() {
    assertThatThrownBy(() -> createSignalNotificationHandler()
        .unregisterListener(mock(SignalListener.class, "SIGALL"), null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("The signal from which to unregister the listener cannot be null!");
  }

  @Test
  public void unregisterListeners() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    SignalListener mockSigQuitListener = mock(SignalListener.class, "SIGQUIT");
    SignalListener mockSigTermListener = mock(SignalListener.class, "SIGTERM");
    SignalListener mockSigTermQuitListener = mock(SignalListener.class, "SIGTERM + SIGQUIT");

    assertThat(signalHandler.isListening(mockSigQuitListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermQuitListener)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
    }

    // register sigquit and sigterm listeners...
    assertThat(signalHandler.registerListener(mockSigQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigTermListener, Signal.SIGTERM)).isTrue();
    assertThat(signalHandler.registerListener(mockSigTermQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigTermQuitListener, Signal.SIGTERM)).isTrue();

    assertThat(signalHandler.isListening(mockSigQuitListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGTERM)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGQUIT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGTERM)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermQuitListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGTERM)).isTrue();
    assertThat(signalHandler.hasListeners(Signal.SIGINT)).isFalse();
    assertThat(signalHandler.hasListeners(Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.hasListeners(Signal.SIGTERM)).isTrue();

    // unregister all sigterm listeners...
    assertThat(signalHandler.unregisterListeners(Signal.SIGTERM)).isTrue();

    assertThat(signalHandler.isListening(mockSigQuitListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitListener, Signal.SIGTERM)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGQUIT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermListener, Signal.SIGTERM)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermQuitListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGINT)).isFalse();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.isListening(mockSigTermQuitListener, Signal.SIGTERM)).isFalse();
    assertThat(signalHandler.hasListeners(Signal.SIGINT)).isFalse();
    assertThat(signalHandler.hasListeners(Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.hasListeners(Signal.SIGTERM)).isFalse();
  }

  @Test
  public void unregisterListenersWithNullSignalThrowsNullPointerException() {
    assertThatThrownBy(() -> createSignalNotificationHandler().unregisterListeners(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("The signal from which to unregister all listeners cannot be null!");
  }

  @Test
  public void notifyListeners() {
    AbstractSignalNotificationHandler signalHandler = createSignalNotificationHandler();

    SignalListener mockSigAllListener = mock(SignalListener.class, "SIGALL");
    SignalListener mockSigIntListener = mock(SignalListener.class, "SIGINT");
    SignalListener mockSigQuitListener = mock(SignalListener.class, "SIGQUIT");
    SignalListener mockSigQuitTermListener = mock(SignalListener.class, "SIGQUIT + SIGTERM");

    SignalEvent sigintEvent = new SignalEvent(this, Signal.SIGINT);
    SignalEvent sigioEvent = new SignalEvent(this, Signal.SIGIO);
    SignalEvent sigquitEvent = new SignalEvent(this, Signal.SIGQUIT);
    SignalEvent sigtermEvent = new SignalEvent(this, Signal.SIGTERM);

    assertThat(signalHandler.isListening(mockSigAllListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigIntListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigQuitListener)).isFalse();
    assertThat(signalHandler.isListening(mockSigQuitTermListener)).isFalse();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isFalse();
    }

    assertThat(signalHandler.registerListener(mockSigAllListener)).isTrue();
    assertThat(signalHandler.registerListener(mockSigIntListener, Signal.SIGINT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigQuitListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigQuitTermListener, Signal.SIGQUIT)).isTrue();
    assertThat(signalHandler.registerListener(mockSigQuitTermListener, Signal.SIGTERM)).isTrue();
    assertThat(signalHandler.isListening(mockSigAllListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigIntListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitListener)).isTrue();
    assertThat(signalHandler.isListening(mockSigQuitTermListener)).isTrue();

    for (Signal signal : Signal.values()) {
      assertThat(signalHandler.hasListeners(signal)).isTrue();
      assertThat(signalHandler.isListening(mockSigAllListener, signal)).isTrue();

      switch (signal) {
        case SIGINT:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isTrue();
          assertThat(signalHandler.isListening(mockSigQuitListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitTermListener, signal)).isFalse();
          break;
        case SIGQUIT:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitListener, signal)).isTrue();
          assertThat(signalHandler.isListening(mockSigQuitTermListener, signal)).isTrue();
          break;
        case SIGTERM:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitTermListener, signal)).isTrue();
          break;
        default:
          assertThat(signalHandler.isListening(mockSigIntListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitListener, signal)).isFalse();
          assertThat(signalHandler.isListening(mockSigQuitTermListener, signal)).isFalse();
      }
    }

    signalHandler.notifyListeners(sigintEvent);
    signalHandler.notifyListeners(sigioEvent);
    signalHandler.notifyListeners(sigquitEvent);
    signalHandler.notifyListeners(sigtermEvent);

    verify(mockSigAllListener, times(1)).handle(eq(sigintEvent));
    verify(mockSigAllListener, times(1)).handle(eq(sigioEvent));
    verify(mockSigAllListener, times(1)).handle(eq(sigquitEvent));
    verify(mockSigAllListener, times(1)).handle(eq(sigtermEvent));

    verify(mockSigIntListener, times(1)).handle(eq(sigintEvent));

    verify(mockSigQuitListener, times(1)).handle(eq(sigquitEvent));

    verify(mockSigQuitTermListener, times(1)).handle(eq(sigquitEvent));
    verify(mockSigQuitTermListener, times(1)).handle(eq(sigtermEvent));
  }

  private String signalListenerForSignalCannotBeNullErrorMessage(Signal signal) {
    return String.format(
        "The SignalListener being registered to listen for '%1$s' signals cannot be null!",
        signal.getName());
  }

  private AbstractSignalNotificationHandler createSignalNotificationHandler() {
    return new TestSignalNotificationHandler();
  }

  private static class TestSignalNotificationHandler extends AbstractSignalNotificationHandler {
    // nothing here
  }

}
