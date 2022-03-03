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

package org.apache.geode.management.internal.cli.util;

import org.apache.geode.internal.gfsh.console.GfeConsoleReaderFactory.GfeConsoleReader;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 * {@link GfeConsoleReader} implementation which uses JLine's Console Reader.
 *
 * Using the default {@link GfeConsoleReader} which uses {@link java.io.Console} makes the shell
 * repeat the characters twice.
 *
 * TODO - Abhishek: Investigate if stty settings can avoid this?
 *
 * @since GemFire 7.0.1
 */
public class GfshConsoleReader extends GfeConsoleReader {
  private final Gfsh gfsh;

  public GfshConsoleReader() {
    gfsh = Gfsh.getCurrentInstance();
  }

  @Override
  public boolean isSupported() {
    return gfsh != null && !gfsh.isHeadlessMode();
  }

  @Override
  public String readLine(String textToPrompt) {
    String lineRead = null;
    if (isSupported()) {
      lineRead = gfsh.interact(textToPrompt);
    }
    return lineRead;
  }

  @Override
  public char[] readPassword(String textToPrompt) {
    char[] password = null;
    if (isSupported()) {
      String passwordString = gfsh.readWithMask(textToPrompt, '*');
      password = passwordString.toCharArray();
    }
    return password;
  }
}
