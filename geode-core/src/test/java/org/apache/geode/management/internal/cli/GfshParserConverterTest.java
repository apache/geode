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

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.event.ParseResult;

import org.apache.geode.management.internal.cli.converters.DiskStoreNameConverter;
import org.apache.geode.management.internal.cli.converters.FilePathConverter;
import org.apache.geode.management.internal.cli.converters.FilePathStringConverter;
import org.apache.geode.management.internal.cli.converters.RegionPathConverter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(IntegrationTest.class)
public class GfshParserConverterTest {

  private GfshParserRule.CommandCandidate commandCandidate;

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
    assertThat(result.getParamValueAsString("dir"))
        .isEqualTo("/testCreateDiskStore1.1#1452637463,/testCreateDiskStore1.2");
  }

  @Test
  public void testEmptyKey() throws Exception {
    String command = "remove  --key=\"\" --region=/GemfireDataCommandsTestRegion";
    GfshParseResult result = parser.parse(command);
    assertThat(result).isNotNull();
    assertThat(result.getParamValueAsString("key")).isEqualTo("");
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
    commandCandidate = parser.complete(command);
    Set<String> commands = parser.getCommandManager().getHelper().getCommands();
    assertThat(commandCandidate.size()).isEqualTo(commands.size());
  }

  @Test
  public void testHelpConverter() {
    String command = "help --command=conn";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(1);
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo(command + "ect");
  }

  @Test
  public void testHintConverter() {
    String command = "hint --topic=";
    commandCandidate = parser.complete(command);
    Set<String> topics = parser.getCommandManager().getHelper().getTopicNames();
    assertThat(commandCandidate.size()).isEqualTo(topics.size());
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo("hint --topic=Client");
  }

  @Test
  public void testDiskStoreNameConverter() throws Exception {
    // spy the DiskStoreNameConverter
    DiskStoreNameConverter spy = parser.spyConverter(DiskStoreNameConverter.class);

    Set<String> diskStores = Arrays.stream("name1,name2".split(",")).collect(Collectors.toSet());
    doReturn(diskStores).when(spy).getCompletionValues();

    String command = "compact disk-store --name=";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(2);

  }

  @Test
  public void testFilePathConverter() throws Exception {
    FilePathStringConverter spy = parser.spyConverter(FilePathStringConverter.class);
    List<String> roots = Arrays.stream("/vol,/logs".split(",")).collect(Collectors.toList());
    List<String> siblings =
        Arrays.stream("sibling1,sibling11,test1".split(",")).collect(Collectors.toList());
    doReturn(roots).when(spy).getRoots();
    doReturn(siblings).when(spy).getSiblings(any());

    String command = "start server --cache-xml-file=";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(2);
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo(command + "/logs");

    command = "start server --cache-xml-file=sibling";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(2);
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo(command + "1");

    FilePathConverter spyFilePathConverter = parser.spyConverter(FilePathConverter.class);
    spyFilePathConverter.setDelegate(spy);
    command = "run --file=test";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(1);
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo(command + "1");
  }


  @Test
  public void testRegionPathConverter() throws Exception {
    RegionPathConverter spy = parser.spyConverter(RegionPathConverter.class);
    Set<String> regions = Arrays.stream("/regionA,/regionB".split(",")).collect(Collectors.toSet());
    doReturn(regions).when(spy).getAllRegionPaths();

    String command = "describe region --name=";
    commandCandidate = parser.complete(command);
    assertThat(commandCandidate.size()).isEqualTo(regions.size());
    assertThat(commandCandidate.getFirstCandidate()).isEqualTo(command + "/regionA");
  }

}
