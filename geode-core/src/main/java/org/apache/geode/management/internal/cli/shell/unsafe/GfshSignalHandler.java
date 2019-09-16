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
package org.apache.geode.management.internal.cli.shell.unsafe;

import java.io.IOException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;

import jline.console.ConsoleReader;

import org.apache.geode.internal.process.signal.AbstractSignalNotificationHandler;
import org.apache.geode.internal.process.signal.Signal;
import org.apache.geode.internal.process.signal.SignalEvent;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.unsafe.internal.sun.misc.SignalHandler;

/**
 * This class externalizes signal handling in order to make the GemFire build process a bit cleaner
 * - for example we have to have exceptions for sun.misc classes when building javadocs.
 * </p>
 *
 * @see org.apache.geode.internal.process.signal.AbstractSignalNotificationHandler
 * @see org.apache.geode.internal.process.signal.Signal
 * @see sun.misc.Signal
 * @see sun.misc.SignalHandler
 */
@SuppressWarnings("unused")
public class GfshSignalHandler extends AbstractSignalNotificationHandler implements SignalHandler {

  private final Map<Signal, SignalHandler> originalSignalHandlers =
      Collections.synchronizedMap(new Hashtable<>(Signal.values().length));

  public GfshSignalHandler() {
    for (final Signal signal : Signal.values()) {
      if (Signal.SIGINT.equals(signal)) {
        originalSignalHandlers.put(signal, org.apache.geode.unsafe.internal.sun.misc.Signal
            .handle(new org.apache.geode.unsafe.internal.sun.misc.Signal(signal.getName()), this));
      }
    }
  }

  @Override
  public void handle(final org.apache.geode.unsafe.internal.sun.misc.Signal sig) {
    notifyListeners(new SignalEvent(sig, Signal.valueOfName(sig.getName())));
    try {
      handleDefault(sig, Gfsh.getConsoleReader());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected void handleDefault(final org.apache.geode.unsafe.internal.sun.misc.Signal sig,
      final ConsoleReader consoleReader)
      throws IOException {
    final Signal signal = Signal.valueOfName(sig.getName());
    switch (signal) {
      case SIGINT:
        if (consoleReader != null) {
          String prompt = consoleReader.getPrompt();
          consoleReader.resetPromptLine(prompt, "", -1);
        }
        break;
      default:
        final SignalHandler handler = getOriginalSignalHandler(signal);
        if (handler != null) {
          handler.handle(sig);
        }
    }
  }

  private SignalHandler getOriginalSignalHandler(final Signal signal) {
    final SignalHandler handler = originalSignalHandlers.get(signal);
    return (handler == SignalHandler.SIG_DFL || handler == SignalHandler.SIG_IGN ? null : handler);
  }

}
