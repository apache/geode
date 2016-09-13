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
