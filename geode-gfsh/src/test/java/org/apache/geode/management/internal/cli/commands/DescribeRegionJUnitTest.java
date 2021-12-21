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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.cache.DataPolicy.NORMAL;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.Scope.DISTRIBUTED_ACK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.RegionDescriptionPerMember;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeRegionJUnitTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DescribeRegionCommand command;
  private static final String COMMAND = "describe region";
  private List<RegionDescriptionPerMember> functionResults;
  private static final String regionName = "testRegion";

  @Before
  public void setup() {
    command = spy(DescribeRegionCommand.class);
    functionResults = new ArrayList<>();
    doReturn(functionResults).when(command).getFunctionResultFromMembers(any());
  }

  private RegionDescriptionPerMember createRegionDescriptionPerMember(String memberName,
      Map<String, String> evictionMap, Map<String, String> partitionMap,
      Map<String, String> regionMap) {
    RegionDescriptionPerMember descriptionPerMember = mock(RegionDescriptionPerMember.class);
    when(descriptionPerMember.getNonDefaultEvictionAttributes()).thenReturn(evictionMap);
    when(descriptionPerMember.getNonDefaultPartitionAttributes()).thenReturn(partitionMap);
    when(descriptionPerMember.getNonDefaultRegionAttributes()).thenReturn(regionMap);
    when(descriptionPerMember.getHostingMember()).thenReturn(memberName);
    when(descriptionPerMember.getScope()).thenReturn(DISTRIBUTED_ACK);
    when(descriptionPerMember.getDataPolicy()).thenReturn(NORMAL);
    when(descriptionPerMember.getName()).thenReturn(regionName);

    return descriptionPerMember;
  }

  @Test
  public void nameIsMandatory() throws Exception {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void regionPathConverted() throws Exception {
    GfshParseResult parseResult = gfsh.parse(COMMAND + " --name=test");
    assertThat(parseResult.getParamValueAsString("name")).isEqualTo(SEPARATOR + "test");
  }

  @Test
  public void gettingDescriptionFromOneMember() throws Exception {
    Map<String, String> evictionAttr = new HashMap<>();
    Map<String, String> partitionAttr = new HashMap<>();
    Map<String, String> regionAttr = new HashMap<>();

    evictionAttr.put("evictKey", "evictVal");
    partitionAttr.put("partKey", "partVal");
    regionAttr.put("regKey", "regVal");

    RegionDescriptionPerMember descriptionPerMember =
        createRegionDescriptionPerMember("mockA", evictionAttr, partitionAttr, regionAttr);
    functionResults.add(descriptionPerMember);

    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat(command, COMMAND + " --name=" + regionName).statusIsSuccess()
            .doesNotContainOutput("Non-Default Attributes Specific To");

    commandAssert.hasDataSection("region-1").hasContent().containsEntry("Name", "testRegion")
        .containsEntry("Data Policy", "normal")
        .containsEntry("Hosting Members", "mockA");
    commandAssert.hasTableSection("non-default-1").hasRowSize(3).hasColumns()
        .containsExactly("Type", "Name", "Value")
        .hasAnyRow().containsExactly("Region", "regKey", "regVal")
        .hasAnyRow().containsExactly("Eviction", "evictKey", "evictVal")
        .hasAnyRow().containsExactly("Partition", "partKey", "partVal");
    commandAssert.hasTableSection("member-non-default-1").isEmpty();
  }

  @Test
  public void gettingDescriptionFromTwoIdenticalMembers() throws Exception {
    Map<String, String> evictionAttr = new HashMap<>();
    Map<String, String> partitionAttr = new HashMap<>();
    Map<String, String> regionAttr = new HashMap<>();

    evictionAttr.put("evictKey", "evictVal");
    partitionAttr.put("partKey", "partVal");
    regionAttr.put("regKey", "regVal");

    RegionDescriptionPerMember descriptionPerMemberA =
        createRegionDescriptionPerMember("mockA", evictionAttr, partitionAttr, regionAttr);
    RegionDescriptionPerMember descriptionPerMemberB =
        createRegionDescriptionPerMember("mockB", evictionAttr, partitionAttr, regionAttr);
    functionResults.add(descriptionPerMemberA);
    functionResults.add(descriptionPerMemberB);

    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat(command, COMMAND + " --name=" + regionName).statusIsSuccess()
            .doesNotContainOutput("Non-Default Attributes Specific To");

    commandAssert.hasDataSection("region-1").hasContent().containsEntry("Name", "testRegion")
        .containsEntry("Data Policy", "normal")
        .extracting(DescribeRegionJUnitTest::extractHostingMembers)
        .asList()
        .containsExactlyInAnyOrder("mockA", "mockB");
    commandAssert.hasTableSection("non-default-1").hasRowSize(3).hasColumns()
        .containsExactly("Type", "Name", "Value")
        .hasAnyRow().containsExactly("Region", "regKey", "regVal")
        .hasAnyRow().containsExactly("Eviction", "evictKey", "evictVal")
        .hasAnyRow().containsExactly("Partition", "partKey", "partVal");
    commandAssert.hasTableSection("member-non-default-1").isEmpty();
  }

  @Test
  public void gettingDescriptionFromTwoDifferentMembers() throws Exception {
    Map<String, String> evictionAttrA = new HashMap<>();
    Map<String, String> partitionAttrA = new HashMap<>();
    Map<String, String> regionAttrA = new HashMap<>();

    evictionAttrA.put("sharedEvictionKey", "sharedEvictionValue");
    partitionAttrA.put("sharedPartitionKey", "uniquePartitionValue_A");
    regionAttrA.put("uniqueRegionKey_A", "uniqueRegionValue_A");

    Map<String, String> evictionAttrB = new HashMap<>();
    Map<String, String> partitionAttrB = new HashMap<>();
    Map<String, String> regionAttrB = new HashMap<>();

    evictionAttrB.put("sharedEvictionKey", "sharedEvictionValue");
    partitionAttrB.put("sharedPartitionKey", "uniquePartitionValue_B");
    regionAttrB.put("uniqueRegionKey_B", "uniqueRegionValue_B");

    RegionDescriptionPerMember descriptionPerMemberA =
        createRegionDescriptionPerMember("mockA", evictionAttrA, partitionAttrA, regionAttrA);
    RegionDescriptionPerMember descriptionPerMemberB =
        createRegionDescriptionPerMember("mockB", evictionAttrB, partitionAttrB, regionAttrB);
    functionResults.add(descriptionPerMemberA);
    functionResults.add(descriptionPerMemberB);

    CommandResultAssert commandAssert =
        gfsh.executeAndAssertThat(command, COMMAND + " --name=" + regionName).statusIsSuccess();

    commandAssert.hasDataSection("region-1").hasContent().containsEntry("Name", "testRegion")
        .containsEntry("Data Policy", "normal")
        .extracting(DescribeRegionJUnitTest::extractHostingMembers)
        .asList()
        .containsExactlyInAnyOrder("mockA", "mockB");
    commandAssert.hasTableSection("non-default-1").hasRowSize(1).hasColumns()
        .containsExactly("Type", "Name", "Value").hasAnyRow()
        .containsExactly("Eviction", "sharedEvictionKey", "sharedEvictionValue");
    commandAssert.hasTableSection("member-non-default-1").hasRowSize(4).hasColumns()
        .containsExactly("Member", "Type", "Name", "Value")
        .hasAnyRow().containsExactly("mockA", "Region", "uniqueRegionKey_A", "uniqueRegionValue_A")
        .hasAnyRow()
        .containsExactly("", "Partition", "sharedPartitionKey", "uniquePartitionValue_A")
        .hasAnyRow().containsExactly("mockB", "Region", "uniqueRegionKey_B", "uniqueRegionValue_B")
        .hasAnyRow()
        .containsExactly("", "Partition", "sharedPartitionKey", "uniquePartitionValue_B");
  }

  static List<String> extractHostingMembers(Map<String, String> map) {
    String key = "Hosting Members";
    assertThat(map).containsKeys(key);
    return Arrays.asList(map.get(key).split("\n"));
  }
}
