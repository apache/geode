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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.jline.reader.Candidate;
import org.jline.reader.ParsedLine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.management.internal.cli.Completion;
import org.apache.geode.management.internal.cli.GfshParser;

/**
 * Unit tests for {@link GfshCompleter}.
 *
 * JLine 3.x migration: This completer bridges GfshParser completion logic with JLine 3's Completer
 * API.
 * JLine 2.x used a different completion interface, so this adapter converts between
 * GfshParser's Completion objects and JLine 3's Candidate objects for command-line auto-completion.
 */
public class GfshCompleterTest {

  private GfshParser mockParser;
  private GfshCompleter completer;

  @Before
  public void setUp() {
    mockParser = mock(GfshParser.class);
    completer = new GfshCompleter(mockParser);
  }

  @Test
  public void testConstructorWithNullParserThrowsException() {
    assertThatThrownBy(() -> new GfshCompleter(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cannot be null");
  }

  @Test
  public void testCompleteConvertsCompletionsToCandidates() {
    // Setup mock parser to return completions
    List<Completion> mockCompletions = Arrays.asList(
        new Completion("APPLY"),
        new Completion("STAGE"));

    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            List<Completion> completions = invocation.getArgument(2);
            completions.addAll(mockCompletions);
            return 43;
          }
        });

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line()).thenReturn("import cluster-configuration --action=");
    when(line.cursor()).thenReturn(43);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify
    assertThat(candidates).hasSize(2);
    assertThat(candidates.get(0).value()).isEqualTo("APPLY");
    assertThat(candidates.get(0).displ()).isEqualTo("APPLY");
    assertThat(candidates.get(1).value()).isEqualTo("STAGE");
    assertThat(candidates.get(1).displ()).isEqualTo("STAGE");
  }

  @Test
  public void testCompleteWithEmptyCompletions() {
    // Setup mock parser to return no completions
    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenReturn(-1);

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line()).thenReturn("invalid command");
    when(line.cursor()).thenReturn(15);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify
    assertThat(candidates).isEmpty();
  }

  @Test
  public void testCompleteWithNullParsedLine() {
    // Execute with null ParsedLine
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, null, candidates);

    // Verify no exception and no candidates
    assertThat(candidates).isEmpty();
  }

  @Test
  public void testCompleteCallsParserWithCorrectParameters() {
    // Setup mock parser
    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenReturn(43);

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line()).thenReturn("test command");
    when(line.cursor()).thenReturn(12);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify parser was called with correct parameters
    verify(mockParser).completeAdvanced(
        eq("test command"),
        eq(12),
        anyList());
  }

  @Test
  public void testCompleteSetsCompleteFlag() {
    // Setup mock parser to return a completion
    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            List<Completion> completions = invocation.getArgument(2);
            completions.add(new Completion("VALUE"));
            return 10;
          }
        });

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line()).thenReturn("command --");
    when(line.cursor()).thenReturn(10);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify complete flag is true
    assertThat(candidates).hasSize(1);
    assertThat(candidates.get(0).complete()).isTrue();
  }

  @Test
  public void testCompleteWithMultipleCompletions() {
    // Setup mock parser to return multiple completions
    List<Completion> mockCompletions = Arrays.asList(
        new Completion("KEY"),
        new Completion("PARTITION"),
        new Completion("THREAD"));

    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            List<Completion> completions = invocation.getArgument(2);
            completions.addAll(mockCompletions);
            return 50;
          }
        });

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line())
        .thenReturn("create gateway-sender --id=test --order-policy=");
    when(line.cursor()).thenReturn(50);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify all completions converted
    assertThat(candidates).hasSize(3);
    assertThat(candidates).extracting(Candidate::value)
        .containsExactly("KEY", "PARTITION", "THREAD");
  }

  @Test
  public void testCompletePreservesCompletionOrder() {
    // Setup mock parser to return completions in specific order
    List<Completion> mockCompletions = Arrays.asList(
        new Completion("APPLY"),
        new Completion("STAGE"));

    when(mockParser.completeAdvanced(anyString(), anyInt(), anyList()))
        .thenAnswer(new Answer<Integer>() {
          @Override
          public Integer answer(InvocationOnMock invocation) {
            List<Completion> completions = invocation.getArgument(2);
            completions.addAll(mockCompletions);
            return 20;
          }
        });

    // Mock ParsedLine
    ParsedLine line = mock(ParsedLine.class);
    when(line.line()).thenReturn("command --option=");
    when(line.cursor()).thenReturn(20);

    // Execute
    List<Candidate> candidates = new ArrayList<>();
    completer.complete(null, line, candidates);

    // Verify order preserved
    assertThat(candidates).extracting(Candidate::value)
        .containsExactly("APPLY", "STAGE");
  }
}
