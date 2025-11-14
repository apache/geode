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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.InOrder;

import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DeploymentInfoTableUtilTest {

  @Test
  public void testGetDeploymentInfoFromFunctionResults_EmptyList() {
    List<CliFunctionResult> functionResults = new ArrayList<>();

    List<DeploymentInfo> result =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(result).isEmpty();
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_WithMapResults() {
    // Test backwards compatibility with Map-based results (pre-1.14 format)
    Map<String, String> deploymentMap = new HashMap<>();
    deploymentMap.put("test.jar", "/path/to/test.jar");
    deploymentMap.put("app.jar", "/path/to/app.jar");

    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn(deploymentMap);
    when(result.getMemberIdOrName()).thenReturn("member1");

    List<CliFunctionResult> functionResults = Arrays.asList(result);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).hasSize(2);
    assertThat(deploymentInfos).extracting(DeploymentInfo::getMemberName)
        .containsExactlyInAnyOrder("member1", "member1");
    assertThat(deploymentInfos).extracting(DeploymentInfo::getFileName)
        .containsExactlyInAnyOrder("test.jar", "app.jar");
    assertThat(deploymentInfos).extracting(DeploymentInfo::getAdditionalDeploymentInfo)
        .containsExactlyInAnyOrder("/path/to/test.jar", "/path/to/app.jar");
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_WithListResults() {
    // Test current format with List-based results (1.14+ format)
    List<DeploymentInfo> deploymentList = Arrays.asList(
        new DeploymentInfo("member1", "test.jar", "/path/to/test.jar"),
        new DeploymentInfo("member1", "app.jar", "/path/to/app.jar"));

    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn(deploymentList);

    List<CliFunctionResult> functionResults = Arrays.asList(result);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).hasSize(2);
    assertThat(deploymentInfos).extracting(DeploymentInfo::getMemberName)
        .containsExactlyInAnyOrder("member1", "member1");
    assertThat(deploymentInfos).extracting(DeploymentInfo::getFileName)
        .containsExactlyInAnyOrder("test.jar", "app.jar");
    assertThat(deploymentInfos).extracting(DeploymentInfo::getAdditionalDeploymentInfo)
        .containsExactlyInAnyOrder("/path/to/test.jar", "/path/to/app.jar");
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_MixedResultTypes() {
    // Test edge case with mixed result types (Map and List)

    // Map-based result (backwards compatibility)
    Map<String, String> deploymentMap = new HashMap<>();
    deploymentMap.put("legacy.jar", "/path/to/legacy.jar");

    CliFunctionResult mapResult = mock(CliFunctionResult.class);
    when(mapResult.getResultObject()).thenReturn(deploymentMap);
    when(mapResult.getMemberIdOrName()).thenReturn("member1");

    // List-based result (current format)
    List<DeploymentInfo> deploymentList = Arrays.asList(
        new DeploymentInfo("member2", "modern.jar", "/path/to/modern.jar"));

    CliFunctionResult listResult = mock(CliFunctionResult.class);
    when(listResult.getResultObject()).thenReturn(deploymentList);

    List<CliFunctionResult> functionResults = Arrays.asList(mapResult, listResult);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).hasSize(2);
    assertThat(deploymentInfos).extracting(DeploymentInfo::getMemberName)
        .containsExactlyInAnyOrder("member1", "member2");
    assertThat(deploymentInfos).extracting(DeploymentInfo::getFileName)
        .containsExactlyInAnyOrder("legacy.jar", "modern.jar");
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_NullMapValues() {
    // Test edge case with null map (should be skipped)
    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn(null);

    List<CliFunctionResult> functionResults = Arrays.asList(result);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).isEmpty();
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_UnsupportedResultType() {
    // Test edge case with unsupported result type (should be ignored)
    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn("unsupported string result");

    List<CliFunctionResult> functionResults = Arrays.asList(result);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).isEmpty();
  }

  @Test
  public void testGetDeploymentInfoFromFunctionResults_PreservesMultipleMembersWithMapFormat() {
    // Test member information preservation with multiple members using Map format
    Map<String, String> member1Map = new HashMap<>();
    member1Map.put("app.jar", "/member1/path/app.jar");

    Map<String, String> member2Map = new HashMap<>();
    member2Map.put("app.jar", "/member2/path/app.jar");
    member2Map.put("util.jar", "/member2/path/util.jar");

    CliFunctionResult result1 = mock(CliFunctionResult.class);
    when(result1.getResultObject()).thenReturn(member1Map);
    when(result1.getMemberIdOrName()).thenReturn("server-1");

    CliFunctionResult result2 = mock(CliFunctionResult.class);
    when(result2.getResultObject()).thenReturn(member2Map);
    when(result2.getMemberIdOrName()).thenReturn("server-2");

    List<CliFunctionResult> functionResults = Arrays.asList(result1, result2);

    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(functionResults);

    assertThat(deploymentInfos).hasSize(3);

    // Verify member information is preserved
    DeploymentInfo server1Info = deploymentInfos.stream()
        .filter(info -> "server-1".equals(info.getMemberName()))
        .findFirst().orElse(null);
    assertThat(server1Info).isNotNull();
    assertThat(server1Info.getFileName()).isEqualTo("app.jar");
    assertThat(server1Info.getAdditionalDeploymentInfo()).isEqualTo("/member1/path/app.jar");

    List<DeploymentInfo> server2Infos = deploymentInfos.stream()
        .filter(info -> "server-2".equals(info.getMemberName()))
        .toList();
    assertThat(server2Infos).hasSize(2);
    assertThat(server2Infos).extracting(DeploymentInfo::getFileName)
        .containsExactlyInAnyOrder("app.jar", "util.jar");
  }

  @Test
  public void testWriteDeploymentInfoToTable_EmptyList() {
    TabularResultModel tabularData = mock(TabularResultModel.class);
    String[] columnHeaders = {"Member", "JAR", "JAR Location"};
    List<DeploymentInfo> deploymentInfos = new ArrayList<>();

    DeploymentInfoTableUtil.writeDeploymentInfoToTable(columnHeaders, tabularData, deploymentInfos);

    // Verify no accumulate calls were made for empty list
    verify(tabularData, org.mockito.Mockito.never()).accumulate(org.mockito.Mockito.any(),
        org.mockito.Mockito.any());
  }

  @Test
  public void testWriteDeploymentInfoToTable_SingleDeployment() {
    TabularResultModel tabularData = mock(TabularResultModel.class);
    String[] columnHeaders = {"Member", "JAR", "JAR Location"};
    List<DeploymentInfo> deploymentInfos = Arrays.asList(
        new DeploymentInfo("server-1", "test.jar", "/path/to/test.jar"));

    DeploymentInfoTableUtil.writeDeploymentInfoToTable(columnHeaders, tabularData, deploymentInfos);

    verify(tabularData).accumulate("Member", "server-1");
    verify(tabularData).accumulate("JAR", "test.jar");
    verify(tabularData).accumulate("JAR Location", "/path/to/test.jar");
  }

  @Test
  public void testWriteDeploymentInfoToTable_MultipleDeployments() {
    TabularResultModel tabularData = mock(TabularResultModel.class);
    String[] columnHeaders = {"Member", "JAR", "JAR Location"};
    List<DeploymentInfo> deploymentInfos = Arrays.asList(
        new DeploymentInfo("server-1", "app.jar", "/server1/path/app.jar"),
        new DeploymentInfo("server-2", "app.jar", "/server2/path/app.jar"),
        new DeploymentInfo("server-1", "util.jar", "/server1/path/util.jar"));

    DeploymentInfoTableUtil.writeDeploymentInfoToTable(columnHeaders, tabularData, deploymentInfos);

    // Verify all entries are written to the table in order
    InOrder inOrder = inOrder(tabularData);

    // First deployment (server-1, app.jar)
    inOrder.verify(tabularData).accumulate("Member", "server-1");
    inOrder.verify(tabularData).accumulate("JAR", "app.jar");
    inOrder.verify(tabularData).accumulate("JAR Location", "/server1/path/app.jar");

    // Second deployment (server-2, app.jar)
    inOrder.verify(tabularData).accumulate("Member", "server-2");
    inOrder.verify(tabularData).accumulate("JAR", "app.jar");
    inOrder.verify(tabularData).accumulate("JAR Location", "/server2/path/app.jar");

    // Third deployment (server-1, util.jar)
    inOrder.verify(tabularData).accumulate("Member", "server-1");
    inOrder.verify(tabularData).accumulate("JAR", "util.jar");
    inOrder.verify(tabularData).accumulate("JAR Location", "/server1/path/util.jar");

    // Verify total counts
    verify(tabularData, times(2)).accumulate("Member", "server-1");
    verify(tabularData, times(1)).accumulate("Member", "server-2");
    verify(tabularData, times(2)).accumulate("JAR", "app.jar");
    verify(tabularData, times(1)).accumulate("JAR", "util.jar");
  }

  @Test
  public void testWriteDeploymentInfoToTable_CustomColumnHeaders() {
    TabularResultModel tabularData = mock(TabularResultModel.class);
    String[] columnHeaders = {"Server", "File", "Path"};
    List<DeploymentInfo> deploymentInfos = Arrays.asList(
        new DeploymentInfo("server-1", "custom.jar", "/custom/path/custom.jar"));

    DeploymentInfoTableUtil.writeDeploymentInfoToTable(columnHeaders, tabularData, deploymentInfos);

    verify(tabularData).accumulate("Server", "server-1");
    verify(tabularData).accumulate("File", "custom.jar");
    verify(tabularData).accumulate("Path", "/custom/path/custom.jar");
  }

  @Test
  public void testIntegration_FlatVsNestedResultStructures() {
    // Integration test demonstrating flat vs nested result processing

    // Create nested results structure (as would come from DeployCommand)
    List<List<CliFunctionResult>> nestedResults = new ArrayList<>();

    // Member 1 results
    List<CliFunctionResult> member1Results = Arrays.asList(
        createMockResultWithMap("member-1", "app1.jar", "/path1/app1.jar"),
        createMockResultWithMap("member-1", "lib1.jar", "/path1/lib1.jar"));

    // Member 2 results
    List<CliFunctionResult> member2Results = Arrays.asList(
        createMockResultWithList("member-2", Arrays.asList(
            new DeploymentInfo("member-2", "app2.jar", "/path2/app2.jar"))));

    nestedResults.add(member1Results);
    nestedResults.add(member2Results);

    // Flatten the nested results (as DeployCommand does internally)
    List<CliFunctionResult> flatResults = new ArrayList<>();
    for (List<CliFunctionResult> memberResults : nestedResults) {
      flatResults.addAll(memberResults);
    }

    // Process the flattened results
    List<DeploymentInfo> deploymentInfos =
        DeploymentInfoTableUtil.getDeploymentInfoFromFunctionResults(flatResults);

    // Verify the results preserve member information from both formats
    assertThat(deploymentInfos).hasSize(3);

    List<DeploymentInfo> member1Deployments = deploymentInfos.stream()
        .filter(info -> "member-1".equals(info.getMemberName()))
        .toList();
    assertThat(member1Deployments).hasSize(2);
    assertThat(member1Deployments).extracting(DeploymentInfo::getFileName)
        .containsExactlyInAnyOrder("app1.jar", "lib1.jar");

    List<DeploymentInfo> member2Deployments = deploymentInfos.stream()
        .filter(info -> "member-2".equals(info.getMemberName()))
        .toList();
    assertThat(member2Deployments).hasSize(1);
    assertThat(member2Deployments.get(0).getFileName()).isEqualTo("app2.jar");
  }

  private CliFunctionResult createMockResultWithMap(String memberName, String jarName,
      String jarPath) {
    Map<String, String> deploymentMap = new HashMap<>();
    deploymentMap.put(jarName, jarPath);

    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn(deploymentMap);
    when(result.getMemberIdOrName()).thenReturn(memberName);
    return result;
  }

  private CliFunctionResult createMockResultWithList(String memberName,
      List<DeploymentInfo> deploymentList) {
    CliFunctionResult result = mock(CliFunctionResult.class);
    when(result.getResultObject()).thenReturn(deploymentList);
    when(result.getMemberIdOrName()).thenReturn(memberName);
    return result;
  }
}
