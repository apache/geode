package org.apache.geode.unsafe.internal.sun.misc;

import java.util.IdentityHashMap;

public class Signal {
  private static IdentityHashMap<SignalHandler, sun.misc.SignalHandler> geodeToSunSignalHandlers =
      new IdentityHashMap<>(4);
  private static IdentityHashMap<sun.misc.SignalHandler, SignalHandler> sunToGeodeSignalHandlers =
      new IdentityHashMap<>(4);

  private final sun.misc.Signal signal;

  public Signal(String name) {
    signal = new sun.misc.Signal(name);
  }

  public int getNumber() {
    return unwrap(this).getNumber();
  }

  public String getName() {
    return unwrap(this).getName();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Signal)) {
      return false;
    }
    return unwrap((Signal) other).equals(unwrap(this));
  }

  @Override
  public int hashCode() {
    return unwrap(this).hashCode();
  }

  @Override
  public String toString() {
    return unwrap(this).toString();
  }

  public static synchronized SignalHandler handle(final Signal signal,
      final SignalHandler signalHandler) {
    return wrap(sun.misc.Signal.handle(unwrap(signal), wrap(signalHandler)));
  }

  private static sun.misc.SignalHandler wrap(final SignalHandler signalHandler) {
    final sun.misc.SignalHandler wrappedSignalHandler =
        geodeToSunSignalHandlers.computeIfAbsent(signalHandler, GeodeSignalHandler::new);
    sunToGeodeSignalHandlers.putIfAbsent(wrappedSignalHandler, signalHandler);
    return wrappedSignalHandler;
  }

  private static Signal wrap(final sun.misc.Signal signal) {
    return new Signal(signal.getName());
  }

  private static sun.misc.Signal unwrap(final Signal signal) {
    return signal.signal;
  }

  private static SignalHandler wrap(final sun.misc.SignalHandler signalHandler) {
    final SignalHandler wrappedSignalHandler =
        sunToGeodeSignalHandlers.computeIfAbsent(signalHandler, SunSignalHandler::new);
    geodeToSunSignalHandlers.putIfAbsent(wrappedSignalHandler, signalHandler);
    return wrappedSignalHandler;
  }

  private static class GeodeSignalHandler implements sun.misc.SignalHandler {
    private final SignalHandler signalHandler;

    GeodeSignalHandler(final SignalHandler signalHandler) {
      this.signalHandler = signalHandler;
    }

    @Override
    public void handle(final sun.misc.Signal signal) {
      signalHandler.handle(wrap(signal));
    }
  }

  private static class SunSignalHandler implements SignalHandler {
    private final sun.misc.SignalHandler signalHandler;

    SunSignalHandler(final sun.misc.SignalHandler signalHandler) {
      this.signalHandler = signalHandler;
    }

    @Override
    public void handle(final Signal signal) {
      signalHandler.handle(unwrap(signal));
    }
  }
}
