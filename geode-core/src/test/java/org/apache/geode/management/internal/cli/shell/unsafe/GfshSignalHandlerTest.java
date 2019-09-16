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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import jline.console.ConsoleReader;
import org.junit.Test;

import org.apache.geode.unsafe.internal.sun.misc.Signal;

/**
 * Unit tests for {@link GfshSignalHandler}.
 *
 */
public class GfshSignalHandlerTest {
  int END_OF_LINE = -1;
  Signal SIGINT = new Signal("INT");
  String PROMPT = "somePrompt";

  @Test
  public void signalHandlerRespondsToSIGINTByClearingPrompt() throws IOException {
    // Interactive attention (CTRL-C). JVM exits normally
    GfshSignalHandler signalHandler = new GfshSignalHandler();

    ConsoleReader consoleReader = mock(ConsoleReader.class);
    when(consoleReader.getPrompt()).thenReturn(PROMPT);

    signalHandler.handleDefault(SIGINT, consoleReader);

    verify(consoleReader, times(1)).resetPromptLine(eq(PROMPT), eq(""), eq(END_OF_LINE));
  }
}
