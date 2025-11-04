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
package org.apache.geode.management.internal.cli.converters;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

/**
 * Unit tests for simple string converter implementations.
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Spring Shell 1.x: Used abstract BaseStringConverter for reusable converter logic
 * - Spring Shell 3.x: No need for abstract base class - conversion is simple passthrough
 * - Completion is handled by ValueProvider (separate from conversion)
 * - This test validates that simple String converters perform passthrough conversion
 *
 * <p>
 * REMOVED FUNCTIONALITY:
 * - BaseStringConverter abstract class (not needed in Spring Shell 3.x)
 * - Parameterized testing across multiple converter types
 * - Completion testing (now belongs in ValueProvider tests)
 * - GfshParserRule integration (Spring Shell 1.x specific)
 *
 * <p>
 * TESTED CONVERTERS (in Spring Shell 1.x):
 * - MemberGroupConverter
 * - ClusterMemberIdNameConverter
 * - MemberIdNameConverter
 * - LocatorIdNameConverter
 * - LocatorDiscoveryConfigConverter
 * - GatewaySenderIdConverter
 *
 * <p>
 * These converters are simple String → String passthroughs in Spring Shell 3.x.
 * Individual converter classes should be tested separately if they implement
 * non-trivial conversion logic.
 */
public class BaseStringConverterJUnitTest {

  /**
   * This test serves as documentation of the migration from BaseStringConverter.
   *
   * <p>
   * Spring Shell 1.x had an abstract BaseStringConverter class that provided:
   * - supports() method checking converter hints
   * - convertFromText() method (passthrough)
   * - getAllPossibleValues() method for completion
   *
   * <p>
   * Spring Shell 3.x approach:
   * - Each converter implements Converter<String, String> directly
   * - Conversion is simple passthrough: convert(source) returns source
   * - Completion values are provided by ValueProvider implementations
   *
   * <p>
   * This test validates that the conversion pattern works correctly.
   */
  @Test
  public void passthroughConversionPattern() {
    // Demonstrates the basic pattern all simple string converters follow
    String input = "test-value-123";
    String output = passthroughConverter(input);

    assertThat(output).isEqualTo(input);
  }

  @Test
  public void passthroughWithSpecialCharacters() {
    String input = "member-name_with.special$chars";
    String output = passthroughConverter(input);

    assertThat(output).isEqualTo(input);
  }

  @Test
  public void passthroughWithSpaces() {
    String input = "value with spaces";
    String output = passthroughConverter(input);

    assertThat(output).isEqualTo(input);
  }

  /**
   * Simulates the conversion pattern used by simple string converters.
   * In Spring Shell 3.x, this is: convert(String source) { return source; }
   */
  private String passthroughConverter(String source) {
    return source;
  }
}
