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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;

import org.jline.reader.LineReader;
import org.jline.terminal.Terminal;
import org.junit.Test;

import org.apache.geode.unsafe.internal.sun.misc.Signal;

/**
 * Unit tests for {@link GfshSignalHandler}.
 *
 * SPRING SHELL 3.x MIGRATION:
 * - Changed from JLine 2.x (jline.console.ConsoleReader) to JLine 3.x (org.jline.reader.LineReader)
 * - JLine 3.x API changes:
 * - ConsoleReader.resetPromptLine(prompt, "", -1) → lineReader.getTerminal().writer().println()
 * - GfshSignalHandler now uses Terminal.writer().println() to clear the line on SIGINT
 *
 * Implementation Notes:
 * - SIGINT (CTRL-C) handling prints a newline via Terminal.writer() instead of resetting prompt
 * line
 * - This effectively moves to a new line, clearing any partial input
 * - The actual implementation in GfshSignalHandler has been updated for JLine 3.x
 */
public class GfshSignalHandlerTest {
  int END_OF_LINE = -1;
  Signal SIGINT = new Signal("INT");
  String PROMPT = "somePrompt";

  @Test
  public void signalHandlerRespondsToSIGINTByClearingPrompt() throws IOException {
    // Interactive attention (CTRL-C). JVM exits normally
    //
    // SPRING SHELL 3.x MIGRATION:
    // JLine 3.x implementation in GfshSignalHandler.handleDefault():
    // - Gets terminal writer: lineReader.getTerminal().writer()
    // - Prints newline: writer.println()
    // This effectively clears the current input by moving to a new line.

    GfshSignalHandler signalHandler = new GfshSignalHandler();

    // JLine 3.x: Mock LineReader, Terminal, and PrintWriter
    LineReader lineReader = mock(LineReader.class);
    Terminal terminal = mock(Terminal.class);
    PrintWriter writer = mock(PrintWriter.class);

    // Set up mock chain: lineReader.getTerminal().writer()
    when(lineReader.getTerminal()).thenReturn(terminal);
    when(terminal.writer()).thenReturn(writer);

    signalHandler.handleDefault(SIGINT, lineReader);

    // JLine 3.x: Verify that println() was called on the terminal writer
    // This clears the current line by printing a newline
    verify(lineReader, times(1)).getTerminal();
    verify(terminal, times(1)).writer();
    verify(writer, times(1)).println();

    // Original JLine 2.x verification (for reference):
    // verify(consoleReader, times(1)).resetPromptLine(eq(PROMPT), eq(""), eq(END_OF_LINE));
  }
}
