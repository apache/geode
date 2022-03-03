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

import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.geode.annotations.Immutable;

/**
 * The AbstractSignalNotificationHandler class...
 *
 * @since GemFire 7.0
 */
public abstract class AbstractSignalNotificationHandler {

  /**
   * @deprecated use the enumerated type instead...
   */
  @Deprecated
  @Immutable
  protected static final List<String> SIGNAL_NAMES;

  // Based on Open BSD OS Signals...
  static {
    String[] SIGNAL_NAMES_ARRAY = new String[] {EMPTY, "HUP", "INT", "QUIT", "ILL", "TRAP", "ABRT",
        "EMT", "FPE", "KILL", "BUS", "SEGV", "SYS", "PIPE", "ALRM", "TERM", "URG", "STOP", "TSTP",
        "CONT", "CHLD", "TTIN", "TTOU", "IO", "XCPU", "XFSZ", "VTALRM", "PROF", "WINCH", "INFO",
        "USR1", "USR2"};

    SIGNAL_NAMES = Collections.unmodifiableList(Arrays.asList(SIGNAL_NAMES_ARRAY));
  }

  @Immutable
  private static final SignalListener LOGGING_SIGNAL_LISTENER =
      event -> System.out.printf("Logging SignalListener Received Signal '%1$s' (%2$d)%n",
          event.getSignal().getName(), event.getSignal().getNumber());

  /**
   * Map used to register SignalListeners with SignalHandlers...
   */
  private final Map<Signal, Set<SignalListener>> signalListeners =
      Collections.synchronizedMap(new HashMap<>(Signal.values().length));

  protected static void assertNotNull(final Object obj, final String message,
      final Object... arguments) {
    if (obj == null) {
      throw new NullPointerException(String.format(message, arguments));
    }
  }

  protected static void assertState(final boolean state, final String message,
      final Object... arguments) {
    if (!state) {
      throw new IllegalStateException(String.format(message, arguments));
    }
  }

  static void assertValidArgument(final boolean valid, final String message,
      final Object... arguments) {
    if (!valid) {
      throw new IllegalArgumentException(String.format(message, arguments));
    }
  }

  protected AbstractSignalNotificationHandler() {
    for (Signal signal : Signal.values()) {
      signalListeners.put(signal, Collections.synchronizedSet(new HashSet<>()));
    }
    // NOTE uncomment for debugging purposes...
    // debug();
  }

  boolean hasListeners(final Signal signal) {
    return !signalListeners.get(signal).isEmpty();
  }

  boolean isListening(final SignalListener listener) {
    boolean registered = false;

    for (Signal signal : Signal.values()) {
      registered |= isListening(listener, signal);
    }

    return registered;
  }

  boolean isListening(final SignalListener listener, final Signal signal) {
    assertNotNull(signal,
        "The signal to determine whether the listener is registered listening for cannot be null!");
    return signalListeners.get(signal).contains(listener);
  }

  protected void notifyListeners(final SignalEvent event) {
    Set<SignalListener> listeners = signalListeners.get(event.getSignal());
    Set<SignalListener> localListeners = Collections.emptySet();

    if (listeners != null) {
      synchronized (listeners) {
        localListeners = new HashSet<>(listeners);
      }
    }

    for (SignalListener listener : localListeners) {
      listener.handle(event);
    }
  }

  public boolean registerListener(final SignalListener listener) {
    assertNotNull(listener,
        "The SignalListener to register, listening for all signals cannot be null!");

    boolean registered = false;

    for (Signal signal : Signal.values()) {
      registered |= registerListener(listener, signal);
    }

    return registered;
  }

  boolean registerListener(final SignalListener listener, final Signal signal) {
    assertNotNull(signal, "The signal to register the listener for cannot be null!");
    assertNotNull(listener,
        "The SignalListener being registered to listen for '%1$s' signals cannot be null!",
        signal.getName());

    return signalListeners.get(signal).add(listener);
  }

  public boolean unregisterListener(final SignalListener listener) {
    boolean unregistered = false;

    for (Signal signal : Signal.values()) {
      unregistered |= unregisterListener(listener, signal);
    }

    return unregistered;
  }

  boolean unregisterListener(final SignalListener listener, final Signal signal) {
    assertNotNull(signal, "The signal from which to unregister the listener cannot be null!");

    return signalListeners.get(signal).remove(listener);
  }

  boolean unregisterListeners(final Signal signal) {
    assertNotNull(signal, "The signal from which to unregister all listeners cannot be null!");

    Set<SignalListener> listeners = signalListeners.get(signal);

    synchronized (listeners) {
      listeners.clear();
      return listeners.isEmpty();
    }
  }

  /**
   * Do not delete.
   */
  private void debug() {
    registerListener(LOGGING_SIGNAL_LISTENER);
  }
}
