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

package org.apache.geode.internal;

import java.io.Console;

import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.util.GfshConsoleReader;

/**
 * Factory for Console Reader Utility.
 * 
 * Default uses <code>java.io.Console</code> returned by
 * <code>System.console()</code>
 * 
 * 
 * @since GemFire 7.0.1
 */
public class GfeConsoleReaderFactory {
  private static GfeConsoleReader defaultConsoleReader = createConsoleReader();
  
  public static final GfeConsoleReader getDefaultConsoleReader() {
    return defaultConsoleReader;
  }
  
  public static final GfeConsoleReader createConsoleReader() {
    GfeConsoleReader consoleReader = null;

    if (CliUtil.isGfshVM()) {
      LogWrapper.getInstance().info("GfeConsoleReaderFactory.createConsoleReader(): isGfshVM");
      consoleReader = new GfshConsoleReader();
      LogWrapper.getInstance().info("GfeConsoleReaderFactory.createConsoleReader(): consoleReader: "+consoleReader+"="+consoleReader.isSupported());
    }
    if (consoleReader == null) {
      consoleReader = new GfeConsoleReader();
    }
    return consoleReader;
  }

  public static class GfeConsoleReader {
    private Console console;
    
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
    
    public char [] readPassword(String textToPrompt) {
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
