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
 *
 */

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class VerifyCommandLine {
  /**
   * Asserts that the java command line contains all and only the specified elements, positioned
   * correctly. The correct positioning is:
   * <ol>
   * <li>All expected java command elements, in the expected sequence, followed by</li>
   * <li>All expected jvm options, in any order, followed by</li>
   * <li>All expected main class command elements, in the expected sequence, followed by</li>
   * <li>All expected main class options, in any order</li>
   * </ol>
   *
   * @param javaCommandLine the command line to verify
   * @param expectedJavaCommandSequence java command and sequence of immediately following options
   * @param expectedJvmOptions options expected in any order between the java command sequence
   *        and the main class sequence
   * @param expectedMainClassSequence main class and sequence of immediately following options
   * @param expectedMainClassOptions options expected in any order after the main class sequence
   */
  static void verifyCommandLine(String[] javaCommandLine, List<String> expectedJavaCommandSequence,
      Set<String> expectedJvmOptions, List<String> expectedMainClassSequence,
      Set<String> expectedMainClassOptions) {
    List<String> commandLine = Arrays.asList(javaCommandLine);

    Set<String> allExpectedElements = new HashSet<>();
    allExpectedElements.addAll(expectedJavaCommandSequence);
    allExpectedElements.addAll(expectedJvmOptions);
    allExpectedElements.addAll(expectedMainClassSequence);
    allExpectedElements.addAll(expectedMainClassOptions);

    assertThat(commandLine).startsWith(expectedJavaCommandSequence.toArray(new String[] {}))
        .containsSequence(expectedMainClassSequence)
        .containsExactlyInAnyOrderElementsOf(allExpectedElements);

    // All JVM options must be between the java command sequence and the main class sequence.
    String mainClassName = expectedMainClassSequence.get(0);
    int mainClassSequenceIndex = commandLine.indexOf(mainClassName);
    int minJvmOptionIndex = expectedJavaCommandSequence.size();
    int maxJvmOptionIndex = minJvmOptionIndex + expectedJvmOptions.size() - 1;
    for (String option : expectedJvmOptions) {
      assertThat(commandLine.indexOf(option))
          .as(() -> String.format("position of JVM option %s", option))
          .isBetween(minJvmOptionIndex, maxJvmOptionIndex);
    }

    // All main class options must be after the main class sequence.
    int minMainClassOptionIndex = mainClassSequenceIndex + expectedMainClassSequence.size();
    int maxMainClassOptionIndex = commandLine.size() - 1;
    for (String option : expectedMainClassOptions) {
      assertThat(commandLine.indexOf(option))
          .as(() -> String.format("position of %s option %s", mainClassName, option))
          .isBetween(minMainClassOptionIndex, maxMainClassOptionIndex);
    }
  }
}
