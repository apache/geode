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
package org.apache.geode.management.internal.cli.shell;

import java.util.ArrayList;
import java.util.List;

import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.GfshParser;

/**
 * JLine 3 Completer implementation for gfsh command-line completion.
 *
 * <p>
 * This class bridges JLine 3's completion API to GfshParser's completion logic.
 * When registered with a LineReader, it enables TAB completion in the interactive gfsh shell.
 *
 * <p>
 * The completion flow:
 * <ol>
 * <li>User presses TAB in the shell
 * <li>JLine 3 calls {@link #complete(LineReader, ParsedLine, List)}
 * <li>We extract the command line and cursor position from ParsedLine
 * <li>We call {@link GfshParser#completeAdvanced(String, int, List)} to get completions
 * <li>We convert {@link Completion} objects to JLine's {@link Candidate} objects
 * <li>JLine displays candidates to the user
 * </ol>
 *
 * <p>
 * Example usage:
 *
 * <pre>
 * GfshParser parser = new GfshParser(commandManager);
 * GfshCompleter completer = new GfshCompleter(parser);
 * LineReader reader = LineReaderBuilder.builder()
 *     .terminal(terminal)
 *     .completer(completer)
 *     .build();
 * </pre>
 *
 * @see GfshParser#completeAdvanced(String, int, List)
 * @see org.jline.reader.Completer
 * @since Spring Shell 3.x
 */
public class GfshCompleter implements Completer {

  private final GfshParser parser;

  /**
   * Creates a new GfshCompleter that uses the specified parser for completion logic.
   *
   * @param parser The GfshParser instance to use for completion logic. Must not be null.
   * @throws IllegalArgumentException if parser is null
   */
  public GfshCompleter(GfshParser parser) {
    if (parser == null) {
      throw new IllegalArgumentException("GfshParser cannot be null");
    }
    this.parser = parser;
  }

  /**
   * Populates completion candidates for the current command line.
   *
   * <p>
   * This method is called by JLine 3 when the user presses TAB. It extracts the command line text
   * and cursor position, delegates to the parser's completion logic, and converts the results to
   * JLine Candidate objects.
   *
   * <p>
   * If the parser returns no completions or encounters an error, the candidates list remains empty
   * and no completions are shown to the user.
   *
   * @param reader The line reader (provides access to terminal and history)
   * @param line The parsed line (provides the command text and cursor position)
   * @param candidates The list to populate with completion candidates (modified by this method)
   */
  @Override
  public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
    if (line == null) {
      return;
    }

    // Get the full command line and cursor position from JLine's ParsedLine
    String commandLine = line.line();
    int cursor = line.cursor();

    // Call the parser's completion logic to get completion suggestions
    List<Completion> completions = new ArrayList<>();
    parser.completeAdvanced(commandLine, cursor, completions);

    // Convert each Completion to a JLine Candidate
    for (Completion completion : completions) {
      // Create a Candidate with the completion value
      // Parameters:
      // - value: The text to insert when user selects this candidate
      // - displ: The text to display in completion menu (same as value)
      // - group: Category for grouping (not used currently, could group by type in future)
      // - descr: Description shown in completion menu (null for now, could enhance Completion class
      // later)
      // - suffix: Text to append after value (not used currently)
      // - key: Unique key for deduplication (not used currently)
      // - complete: Whether this is a complete value (true = don't continue completing)
      Candidate candidate = new Candidate(
          completion.getValue(), // value: what gets inserted
          completion.getValue(), // displ: what's shown in menu
          null, // group: not used yet (could be "Enum Values", "Boolean Values", etc.)
          null, // descr: could enhance Completion class to include descriptions
          null, // suffix: not used (could add space or "=" for options)
          null, // key: not used (JLine uses value for deduplication)
          true // complete: this is a complete value, stop after inserting
      );
      candidates.add(candidate);
    }
  }
}
