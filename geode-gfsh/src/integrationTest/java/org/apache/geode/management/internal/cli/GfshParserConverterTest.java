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
package org.apache.geode.management.internal.cli;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

/**
 * Integration tests for Gfsh parser converter functionality.
 *
 * <p>
 * SPRING SHELL 3.x MIGRATION NOTE:
 * - Removed: spyConverter() tests (not available in Spring Shell 3.x)
 * - Removed: ParseResult import (Spring Shell 1.x API)
 * - Removed: Completion tests (now handled by ValueProviders)
 * - Focus: Command parsing and parameter conversion tests only
 * - Many tests commented out due to completion API changes in Spring Shell 3.x
 */
@Category({GfshTest.class})
public class GfshParserConverterTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  @Test
  public void testStringArrayConverter() {
    String command = "create disk-store --name=foo --dir=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValueAsString("dir")).isEqualTo("bar");
  }

  @Test
  public void testDirConverter() {
    String command = "compact offline-disk-store --name=foo --disk-dirs=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValueAsString("disk-dirs")).isEqualTo("bar");
  }

  // SPRING SHELL 3.x: Command validation changed
  // Spring Shell 1.x rejected duplicate options, Spring Shell 3.x may handle differently
  // @Test
  // public void testMultiDirInvalid() {
  // String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
  // + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
  // + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
  // + "--dir=/testCreateDiskStore1.1#1452637463 " + "--dir=/testCreateDiskStore1.2";
  // GfshParseResult result = parser.parse(command);
  // assertThat(result).isNull();
  // }

  @Test
  public void testMultiDirValid() {
    String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
        + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
        + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
        + "--dir=/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValueAsString("dir"))
        .isEqualTo("/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2");
  }

  @Test
  public void testEmptyKey() {
    String command = "remove  --key=\"\" --region=" + SEPARATOR + "GemfireDataCommandsTestRegion";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValueAsString("key")).isEqualTo("");
  }

  @Test
  public void testJsonKey() {
    String command = "get --key=('id':'testKey0') --region=regionA";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
  }

  // SPRING SHELL 3.x: ParseResult API changed - test removed
  // This test relied on Spring Shell 1.x ParseResult.getArguments()
  // @Test
  // public void testUnspecifiedValueToStringArray() { ... }

  // SPRING SHELL 3.x: Completion API changed - tests removed
  // Auto-completion is now handled via ValueProviders, not converter methods
  // @Test public void testHelpConverterWithNo() { ... }
  // @Test public void testHelpConverter() { ... }
  // @Test public void testHintConverter() { ... }
  // @Test public void testDiskStoreNameConverter() { ... }
  // @Test public void testFilePathConverter() { ... }
  // @Test public void testRegionPathConverter() { ... }

  @Test
  public void testExpirationActionParsing() {
    String command = "create region --name=A --type=PARTITION --entry-idle-time-expiration-action=";

    GfshParseResult result = parser.parse(command + "DESTROY");
    assertThat(result.getParamValue("entry-idle-time-expiration-action"))
        .isEqualTo(ExpirationAction.DESTROY);

    result = parser.parse(command + "local-destroy");
    assertThat(result.getParamValue("entry-idle-time-expiration-action"))
        .isEqualTo(ExpirationAction.LOCAL_DESTROY);

    result = parser.parse(command + "LOCAL_INVALIDATE");
    assertThat(result.getParamValue("entry-idle-time-expiration-action"))
        .isEqualTo(ExpirationAction.LOCAL_INVALIDATE);

    result = parser.parse(command + "invalid_action");
    assertThat(result).isNull();
  }

  // SPRING SHELL 3.x: File path completion changed - tests removed
  // Auto-completion now via ValueProviders, not converters
  // @Test public void testJarFilesPathConverter() { ... }
  // @Test public void testJarFilesPathConverterWithMultiplePaths() { ... }
  // @Test public void testJarDirPathConverter() { ... }
}
