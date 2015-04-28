/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell.unsafe;

import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import com.gemstone.gemfire.internal.process.signal.AbstractSignalNotificationHandler;
import com.gemstone.gemfire.internal.process.signal.Signal;
import com.gemstone.gemfire.internal.process.signal.SignalEvent;

import sun.misc.SignalHandler;

/**
 * This class externalizes signal handling in order to make the GemFire build process a bit cleaner - for example
 * we have to have exceptions for sun.misc classes when building javadocs.
 * </p>
 * @author jdeppe
 * @author jblum
 * @see com.gemstone.gemfire.internal.process.signal.AbstractSignalNotificationHandler
 * @see com.gemstone.gemfire.internal.process.signal.Signal
 * @see sun.misc.Signal
 * @see sun.misc.SignalHandler
 */
@SuppressWarnings("unused")
public class GfshSignalHandler extends AbstractSignalNotificationHandler implements SignalHandler {

  private final Map<Signal, SignalHandler> originalSignalHandlers = Collections.synchronizedMap(
    new Hashtable<Signal, SignalHandler>(Signal.values().length));

  public GfshSignalHandler() {
    for (final Signal signal : Signal.values()) {
      // NOTE SignalHandler registration for SIGQUIT led to an IllegalArgumentException without the following
      // "if" statement...
      // Exception in thread "main" java.lang.IllegalArgumentException: Signal already used by VM: SIGQUIT
      //if (!Signal.SIGQUIT.equals(signal)) {
      // TODO uncomment above if statement if all Signals need to be handled by this SignalHandler
      if (Signal.SIGINT.equals(signal)) {
        originalSignalHandlers.put(signal, sun.misc.Signal.handle(new sun.misc.Signal(signal.getName()), this));
      }
    }
  }

  @Override
  public void handle(final sun.misc.Signal sig) {
    //System.err.printf("Thread (%1$s) is processing Signal '%2$s' (%3$d)...%n",
    //  Thread.currentThread().getName(), sig.getName(), sig.getNumber());
    notifyListeners(new SignalEvent(sig, Signal.valueOfName(sig.getName())));
    handleDefault(sig);
  }

  protected void handleDefault(final sun.misc.Signal sig) {
    final Signal signal = Signal.valueOfName(sig.getName());
    switch (signal) {
      case SIGINT:
        break; // ignore the interrupt signal
      default:
        final SignalHandler handler = getOriginalSignalHandler(signal);
        if (handler != null) {
          handler.handle(sig);
        }
    }
  }

  protected SignalHandler getOriginalSignalHandler(final Signal signal) {
    final SignalHandler handler = originalSignalHandlers.get(signal);
    return (handler == SignalHandler.SIG_DFL || handler == SignalHandler.SIG_IGN ? null : handler);
  }

}
