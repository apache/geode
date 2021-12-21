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

package org.apache.geode.internal.gfsh.console;

import java.io.Console;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.GfshConsoleReader;

/**
 * Factory for Console Reader Utility.
 *
 * Default uses <code>java.io.Console</code> returned by <code>System.console()</code>
 *
 *
 * @since GemFire 7.0.1
 */
public class GfeConsoleReaderFactory {
  @Immutable
  private static final GfeConsoleReader defaultConsoleReader = createConsoleReader();

  public static GfeConsoleReader getDefaultConsoleReader() {
    return defaultConsoleReader;
  }

  public static GfeConsoleReader createConsoleReader() {
    GfeConsoleReader consoleReader;

    if (Gfsh.getCurrentInstance() != null) {
      LogWrapper logWrapper = Gfsh.getCurrentInstance().getGfshFileLogger();
      logWrapper.info("GfeConsoleReaderFactory.createConsoleReader(): isGfshVM");
      consoleReader = new GfshConsoleReader();
      logWrapper.info("GfeConsoleReaderFactory.createConsoleReader(): consoleReader: "
          + consoleReader + "=" + consoleReader.isSupported());
    } else {
      consoleReader = new GfeConsoleReader();
    }
    return consoleReader;
  }

  public static class GfeConsoleReader {
    private final Console console;

    protected GfeConsoleReader() {
      console = System.console();
    }

    public boolean isSupported() {
      return console != null;
    }

    public String readLine(String textToPrompt) {
      if (isSupported()) {
        return console.readLine(textToPrompt);
      }
      return null;
    }

    public char[] readPassword(String textToPrompt) {
      if (isSupported()) {
        return console.readPassword(textToPrompt);
      }
      return null;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + ": isSupported=" + isSupported();
    }
  }
}
