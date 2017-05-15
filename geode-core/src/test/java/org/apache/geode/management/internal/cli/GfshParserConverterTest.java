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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.apache.geode.management.internal.cli.converters.DiskStoreNameConverter;
import org.apache.geode.management.internal.cli.converters.FilePathConverter;
import org.apache.geode.management.internal.cli.converters.FilePathStringConverter;
import org.apache.geode.management.internal.cli.converters.RegionPathConverter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.event.ParseResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Category(IntegrationTest.class)
public class GfshParserConverterTest {
  private static GfshParser parser;
  private List<Completion> candidates;
  private int cursor;

  @BeforeClass
  public static void setUpClass() throws Exception {
    parser = new GfshParser();
  }

  @Before
  public void setUp() throws Exception {
    this.candidates = new ArrayList<>();
  }

  @Test
  public void testStringArrayConverter() {
    String command = "create disk-store --name=foo --dir=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("dir")).isEqualTo("bar");
  }

  @Test
  public void testDirConverter() {
    String command = "compact offline-disk-store --name=foo --disk-dirs=bar";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("disk-dirs")).isEqualTo("bar");
  }

  @Test
  public void testMultiDirInvalid() throws Exception {
    String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
        + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
        + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
        + "--dir=/testCreateDiskStore1.1#1452637463 " + "--dir=/testCreateDiskStore1.2";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNull();
  }

  @Test
  public void testMultiDirValid() throws Exception {
    String command = "create disk-store --name=testCreateDiskStore1 --group=Group1 "
        + "--allow-force-compaction=true --auto-compact=false --compaction-threshold=67 "
        + "--max-oplog-size=355 --queue-size=5321 --time-interval=2023 --write-buffer-size=3110 "
        + "--dir=/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("dir"))
        .isEqualTo("/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2");
  }

  @Test
  public void testEmptyKey() throws Exception {
    String command = "remove  --key=\"\" --region=/GemfireDataCommandsTestRegion";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValue("key")).isEqualTo("");
  }

  @Test
  public void testJsonKey() throws Exception {
    String command = "get --key=('id':'testKey0') --region=regionA";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
  }

  @Test
  public void testUnspecifiedValueToStringArray() {
    String command = "change loglevel --loglevel=finer --groups=group1,group2";
    ParseResult result = parser.parse(command);
    String[] memberIdValue = (String[]) result.getArguments()[0];
    assertThat(memberIdValue).isNull();
  }

  @Test
  public void testHelpConverterWithNo() {
    String command = "help --command=";
    cursor = parser.completeAdvanced(command, candidates);
    Set<String> commands = parser.getCommandManager().getHelper().getCommands();
    assertThat(candidates.size()).isEqualTo(commands.size());
  }

  @Test
  public void testHelpConverter() {
    String command = "help --command=conn";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates.size()).isEqualTo(1);
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo(command + "ect");
  }

  @Test
  public void testHintConverter() {
    String command = "hint --topic=";
    cursor = parser.completeAdvanced(command, candidates);
    Set<String> topics = parser.getCommandManager().getHelper().getTopicNames();
    assertThat(candidates.size()).isEqualTo(topics.size());
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo("hint --topic=Client");
  }

  @Test
  public void testDiskStoreNameConverter() throws Exception {
    // spy the DiskStoreNameConverter
    DiskStoreNameConverter spy = spyConverter(DiskStoreNameConverter.class);

    Set<String> diskStores = Arrays.stream("name1,name2".split(",")).collect(Collectors.toSet());
    doReturn(diskStores).when(spy).getDiskStoreNames();

    String command = "compact disk-store --name=";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates).hasSize(2);

  }

  @Test
  public void testFilePathConverter() throws Exception {
    FilePathStringConverter spy = spyConverter(FilePathStringConverter.class);
    List<String> roots = Arrays.stream("/vol,/logs".split(",")).collect(Collectors.toList());
    List<String> siblings =
        Arrays.stream("sibling1,sibling11,test1".split(",")).collect(Collectors.toList());
    doReturn(roots).when(spy).getRoots();
    doReturn(siblings).when(spy).getSiblings(any());

    String command = "start server --properties-file=";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates).hasSize(2);
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo(command + "/logs");
    candidates.clear();

    command = "start server --properties-file=sibling";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates).hasSize(2);
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo(command + "1");
    candidates.clear();

    FilePathConverter spyFilePathConverter = spyConverter(FilePathConverter.class);
    spyFilePathConverter.setDelegate(spy);
    command = "run --file=test";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates).hasSize(1);
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo(command + "1");
  }

  @Test
  public void testRegionPathConverter() throws Exception {
    RegionPathConverter spy = spyConverter(RegionPathConverter.class);
    Set<String> regions = Arrays.stream("/regionA,/regionB".split(",")).collect(Collectors.toSet());
    doReturn(regions).when(spy).getAllRegionPaths();

    String command = "describe region --name=";
    cursor = parser.completeAdvanced(command, candidates);
    assertThat(candidates).hasSize(regions.size());
    assertThat(getCompleted(command, cursor, candidates.get(0))).isEqualTo(command + "/regionA");
  }

  private String getCompleted(String buffer, int cursor, Completion completed) {
    return buffer.substring(0, cursor) + completed.getValue();
  }

  private static <T extends Converter> T spyConverter(Class<T> klass) {
    Set<Converter<?>> converters = parser.getConverters();
    T foundConverter = null, spy = null;
    for (Converter converter : converters) {
      if (klass.isAssignableFrom(converter.getClass())) {
        foundConverter = (T) converter;
        break;
      }
    }
    if (foundConverter != null) {
      parser.remove(foundConverter);
      spy = spy(foundConverter);
      parser.add(spy);
    }
    return spy;
  }
}
