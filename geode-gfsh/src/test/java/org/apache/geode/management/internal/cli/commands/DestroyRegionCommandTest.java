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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.configuration.domain.XmlEntity;
import org.apache.geode.management.internal.functions.CliFunctionResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DestroyRegionCommandTest {

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  private DestroyRegionCommand command;
  private CliFunctionResult result1, result2;
  private InternalConfigurationPersistenceService ccService;
  XmlEntity xmlEntity;
  private CacheConfig cacheConfig = mock(CacheConfig.class);
  private RegionConfig regionConfig = mock(RegionConfig.class);
  private CacheElement cacheElement = mock(CacheElement.class);
  private List<RegionConfig> regionConfigList = Collections.singletonList(regionConfig);
  private List<CacheElement> cacheElementList = Collections.singletonList(cacheElement);

  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    xmlEntity = mock(XmlEntity.class);
    command = spy(DestroyRegionCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    doReturn(mock(InternalCache.class)).when(command).getCache();

    List<CliFunctionResult> functionResults = new ArrayList<>();

    doReturn(functionResults).when(command).executeAndGetFunctionResult(any(), any(),
        any(Set.class));
    result1 = mock(CliFunctionResult.class);
    result2 = mock(CliFunctionResult.class);
    when(result1.getMemberIdOrName()).thenReturn("member1");
    when(result2.getMemberIdOrName()).thenReturn("member2");
    functionResults.add(result1);
    functionResults.add(result2);
  }

  @Test
  public void invalidRegion() {
    parser.executeAndAssertThat(command, "destroy region").statusIsError()
        .containsOutput("Invalid command");

    parser.executeAndAssertThat(command, "destroy region --name=").statusIsError()
        .containsOutput("Invalid command");

    parser.executeAndAssertThat(command, "destroy region --name=/").statusIsError()
        .containsOutput("Invalid command");
  }

  @Test
  public void regionConverterApplied() {
    GfshParseResult parseResult = parser.parse("destroy region --name=test");
    assertThat(parseResult.getParamValue("name")).isEqualTo("/test");
  }

  @Test
  public void whenNoRegionIsFoundOnAnyMembers() {
    doReturn(Collections.emptySet()).when(command).findMembersForRegion(any());
    parser.executeAndAssertThat(command, "destroy region --name=test").statusIsError()
        .containsOutput("Could not find a Region with Region path");

    parser.executeAndAssertThat(command, "destroy region --name=test --if-exists")
        .statusIsSuccess();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void multipleResultReturned_oneSucess_oneFailed() {
    // mock this to pass the member search call
    doReturn(Collections.singleton(DistributedMember.class)).when(command)
        .findMembersForRegion(any());
    when(result1.isSuccessful()).thenReturn(true);
    when(result1.getStatusMessage()).thenReturn("result1 message");
    when(result1.getXmlEntity()).thenReturn(xmlEntity);

    when(result2.isSuccessful()).thenReturn(false);
    when(result2.getStatusMessage()).thenReturn("result2 message");

    parser.executeAndAssertThat(command, "destroy region --name=test").statusIsSuccess()
        .containsOutput("result1 message").containsOutput("result2 message");

    // verify that xmlEntiry returned by the result1 is saved to Cluster config
    verify(ccService).deleteXmlEntity(xmlEntity, null);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void multipleResultReturned_oneSuccess_oneException() {
    // mock this to pass the member search call
    doReturn(Collections.singleton(DistributedMember.class)).when(command)
        .findMembersForRegion(any());
    when(result1.isSuccessful()).thenReturn(true);
    when(result1.getStatusMessage()).thenReturn("result1 message");
    when(result1.getXmlEntity()).thenReturn(xmlEntity);

    when(result2.isSuccessful()).thenReturn(false);
    when(result2.getStatusMessage()).thenReturn("something happened");

    parser.executeAndAssertThat(command, "destroy region --name=test").statusIsSuccess()
        .containsOutput("result1 message").containsOutput("something happened");


    // verify that xmlEntiry returned by the result1 is saved to Cluster config
    verify(ccService).deleteXmlEntity(xmlEntity, null);
  }

  @Test
  public void multipleResultReturned_all_failed() {
    // mock this to pass the member search call
    doReturn(Collections.singleton(DistributedMember.class)).when(command)
        .findMembersForRegion(any());
    when(result1.isSuccessful()).thenReturn(false);
    when(result1.getStatusMessage()).thenReturn("result1 message");

    when(result2.isSuccessful()).thenReturn(false);
    when(result2.getStatusMessage()).thenReturn("something happened");

    parser.executeAndAssertThat(command, "destroy region --name=test").statusIsError()
        .containsOutput("result1 message").containsOutput("something happened");


    // verify that xmlEntry returned by the result1 is saved to Cluster config
    verify(ccService, never()).deleteXmlEntity(any(), any());
  }

  private void setupJDBCMappingOnRegion(String regionName) {
    doReturn(cacheConfig).when(ccService).getCacheConfig("cluster");
    doReturn(regionConfigList).when(cacheConfig).getRegions();
    doReturn(regionName).when(regionConfig).getName();
    doReturn(regionName).when(regionConfig).getId();
    doReturn(cacheElementList).when(regionConfig).getCustomRegionElements();
    doReturn("jdbc-mapping").when(cacheElement).getId();
  }

  @Test(expected = IllegalStateException.class)
  public void checkForJDBCMappingWithRegionPathThrowsIllegalStateException() {
    setupJDBCMappingOnRegion("regionName");

    command.checkForJDBCMapping("/regionName");
  }

  @Test(expected = IllegalStateException.class)
  public void checkForJDBCMappingWithRegionNameThrowsIllegalStateException() {
    setupJDBCMappingOnRegion("regionName");

    command.checkForJDBCMapping("regionName");
  }

  @Test(expected = IllegalStateException.class)
  public void checkForJDBCMappingWithRegionNameThrowsIllegalStateExceptionForGroup() {
    Set<String> groups = new HashSet<>();
    groups.add("Group1");
    doReturn(groups).when(ccService).getGroups();
    setupJDBCMappingOnRegion("regionName");
    doReturn(cacheConfig).when(ccService).getCacheConfig("Group1");

    command.checkForJDBCMapping("regionName");
  }

  @Test
  public void checkForJDBCMappingWithNoClusterConfigDoesNotThrowException() {
    setupJDBCMappingOnRegion("regionName");
    doReturn(null).when(command).getConfigurationPersistenceService();

    command.checkForJDBCMapping("regionName");
  }

  @Test
  public void checkForJDBCMappingWithNoCacheConfigDoesNotThrowException() {
    setupJDBCMappingOnRegion("regionName");
    doReturn(null).when(ccService).getCacheConfig("cluster");

    command.checkForJDBCMapping("regionName");
  }

  @Test
  public void checkForJDBCMappingWithNoRegionConfigDoesNotThrowException() {
    setupJDBCMappingOnRegion("regionName");
    doReturn(Collections.emptyList()).when(cacheConfig).getRegions();

    command.checkForJDBCMapping("regionName");
  }

  @Test
  public void checkForJDBCMappingWithNoJDBCMappingDoesNotThrowException() {
    setupJDBCMappingOnRegion("regionName");
    doReturn("something").when(cacheElement).getId();

    command.checkForJDBCMapping("regionName");
  }
}
