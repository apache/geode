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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RegionPathConverter}.
 *
 * <p>
 * Tests region path normalization and validation:
 * <ul>
 * <li>Adding leading separator when missing</li>
 * <li>Preserving paths that already have separator</li>
 * <li>Handling sub-region paths</li>
 * <li>Rejecting invalid inputs (bare separator)</li>
 * </ul>
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * <ul>
 * <li>Removed: getAllPossibleValues() completion tests</li>
 * <li>Removed: supports() method tests (no longer in Converter interface)</li>
 * <li>Removed: GfshParserRule and auto-completion tests</li>
 * <li>Focus: Pure conversion logic only</li>
 * </ul>
 */
public class RegionPathConverterJUnitTest {

  private RegionPathConverter converter;

  @BeforeEach
  public void setUp() {
    converter = new RegionPathConverter();
  }

  @Test
  public void convertBareSeparatorThrowsException() {
    assertThatThrownBy(() -> converter.convert(SEPARATOR))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("invalid region path: " + SEPARATOR);
  }

  @Test
  public void convertAddsLeadingSeparatorWhenMissing() {
    String result = converter.convert("region");
    assertThat(result).isEqualTo(SEPARATOR + "region");
  }

  @Test
  public void convertPreservesPathWithLeadingSeparator() {
    String result = converter.convert(SEPARATOR + "region");
    assertThat(result).isEqualTo(SEPARATOR + "region");
  }

  @Test
  public void convertHandlesSubRegionPath() {
    String result = converter.convert(SEPARATOR + "parent" + SEPARATOR + "child");
    assertThat(result).isEqualTo(SEPARATOR + "parent" + SEPARATOR + "child");
  }

  @Test
  public void convertAddsLeadingSeparatorToSubRegionPath() {
    String result = converter.convert("parent" + SEPARATOR + "child");
    assertThat(result).isEqualTo(SEPARATOR + "parent" + SEPARATOR + "child");
  }

  @Test
  public void convertHandlesComplexRegionNames() {
    String result = converter.convert("my-region_123");
    assertThat(result).isEqualTo(SEPARATOR + "my-region_123");
  }

  @Test
  public void convertNullReturnsNull() {
    String result = converter.convert(null);
    assertThat(result).isNull();
  }
}
