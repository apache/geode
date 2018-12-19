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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.domain.ClassName;
import org.apache.geode.management.internal.cli.functions.CreateRegionFunctionArgs;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class CreateRegionCommandTest {
  @Rule
  public GfshParserRule parser = new GfshParserRule();

  private CreateRegionCommand command;
  private InternalCache cache;
  private DistributedRegionMXBean regionMXBean;
  ManagementService service;

  private static String COMMAND = "create region --name=region --type=REPLICATE ";

  @Before
  public void before() throws Exception {
    command = spy(CreateRegionCommand.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();

    service = mock(ManagementService.class);
    doReturn(service).when(command).getManagementService();
    regionMXBean = mock(DistributedRegionMXBean.class);
    when(service.getDistributedRegionMXBean(any())).thenReturn(regionMXBean);
  }

  @Test
  public void testRegionExistsReturnsCorrectValue() throws Exception {
    assertThat(command.regionExists(null)).isFalse();
  }

  @Test
  public void missingName() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command, "create region");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void missingBothTypeAndUseAttributeFrom() throws Exception {
    CommandResult result =
        parser.executeCommandWithInstance(command, "create region --name=region");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("One of \"type\" or \"template-region\" is required.");
  }

  @Test
  public void haveBothTypeAndUseAttributeFrom() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --template-region=regionB");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("Only one of type & template-region can be specified.");
  }

  @Test
  public void invalidEvictionAction() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-action=invalidAction");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("eviction-action must be 'local-destroy' or 'overflow-to-disk'");
  }

  @Test
  public void invalidEvictionAttributes() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-max-memory=1000 --eviction-entry-count=200");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("eviction-max-memory and eviction-entry-count cannot both be specified.");
  }

  @Test
  public void missingEvictionAction() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-max-memory=1000");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains("eviction-action must be specified.");
  }

  @Test
  public void invalidEvictionSizerAndCount() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-entry-count=1 --eviction-object-sizer=abc --eviction-action=local-destroy");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("eviction-object-sizer cannot be specified with eviction-entry-count");
  }

  @Test
  public void templateRegionAttributesNotAvailable() throws Exception {
    doReturn(null).when(command).getRegionAttributes(eq(cache), any());
    DistributedSystemMXBean dsMBean = mock(DistributedSystemMXBean.class);
    doReturn(dsMBean).when(command).getDSMBean();
    doReturn(new String[] {}).when(dsMBean).listGatewaySenders();
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(true).when(command).regionExists(any());

    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --template-region=regionA");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent())
        .contains("Could not retrieve region attributes for given path");
  }

  @Test
  public void defaultValues() throws Exception {
    ResultCollector resultCollector = mock(ResultCollector.class);
    doReturn(resultCollector).when(command).executeFunction(any(), any(), any(Set.class));
    when(resultCollector.getResult()).thenReturn(Collections.emptyList());
    DistributedSystemMXBean dsMBean = mock(DistributedSystemMXBean.class);
    doReturn(dsMBean).when(command).getDSMBean();
    doReturn(new String[] {}).when(dsMBean).listGatewaySenders();
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());
    doReturn(true).when(command).verifyDistributedRegionMbean(any(), any());
    when(service.getDistributedRegionMXBean(any())).thenReturn(null);

    parser.executeCommandWithInstance(command, "create region --name=A --type=REPLICATE");
    ArgumentCaptor<CreateRegionFunctionArgs> argsCaptor =
        ArgumentCaptor.forClass(CreateRegionFunctionArgs.class);
    verify(command).executeFunction(any(), argsCaptor.capture(), any(Set.class));
    CreateRegionFunctionArgs args = argsCaptor.getValue();

    assertThat(args.getConfig().getRegionAttributes()).isNotNull();
  }

  @Test
  public void invalidCacheListener() throws Exception {
    parser
        .executeAndAssertThat(command,
            "create region --name=region --type=REPLICATE --cache-listener=abc-def")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void invalidCacheLoader() throws Exception {
    parser
        .executeAndAssertThat(command,
            "create region --name=region --type=REPLICATE --cache-loader=abc-def")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void invalidCacheWriter() throws Exception {
    parser
        .executeAndAssertThat(command,
            "create region --name=region --type=REPLICATE --cache-writer=abc-def")
        .statusIsError().containsOutput("Invalid command");
  }

  @Test
  public void declarableClassIsNullIfNotSpecified() throws Exception {
    GfshParseResult result = parser.parse("create region --name=region --cache-writer");
    assertThat(result.getParamValue("cache-writer")).isNull();
    assertThat(result.getParamValue("cache-loader")).isNull();
    assertThat(result.getParamValue("cache-listener")).isNull();
  }

  @Test
  // this is enforced by the parser, if empty string is passed, parser will turn that into null
  // first
  public void declarableClassIsNullWhenEmptyStringIsPassed() {
    GfshParseResult result = parser
        .parse("create region --name=region --cache-writer='' --cache-loader --cache-listener=''");
    assertThat(result.getParamValue("cache-writer")).isNull();
    assertThat(result.getParamValue("cache-loader")).isNull();
    assertThat(result.getParamValue("cache-listener")).isNull();
  }

  @Test
  public void emptySpace() {
    GfshParseResult result = parser
        .parse("create region --name=region --cache-writer=' ' --cache-loader --cache-listener=''");
    assertThat(result.getParamValue("cache-writer")).isEqualTo(ClassName.EMPTY);
    assertThat(result.getParamValue("cache-listener")).isNull();
    assertThat(result.getParamValue("cache-loader")).isNull();
  }

  @Test
  public void parseDeclarableWithClassOnly() {
    GfshParseResult result = parser.parse("create region --name=region --cache-writer=my.abc");
    ClassName writer = (ClassName) result.getParamValue("cache-writer");
    assertThat(writer.getClassName()).isEqualTo("my.abc");
    assertThat(writer.getInitProperties()).isNotNull().isEmpty();
  }

  @Test
  public void parseDeclarableWithClassAndProps() {
    String json = "{'k1':'v1','k2':'v2'}";
    GfshParseResult result =
        parser.parse("create region --name=region --cache-writer=my.abc" + json);
    ClassName writer = (ClassName) result.getParamValue("cache-writer");
    assertThat(writer.getClassName()).isEqualTo("my.abc");
    assertThat(writer.getInitProperties()).containsKeys("k1", "k2");
  }

  @Test
  public void parseDeclarableWithJsonWithSpace() {
    String json = "{'k1' : 'v   1', 'k2' : 'v2'}";
    GfshParseResult result =
        parser.parse("create region --name=region --cache-writer=\"my.abc" + json + "\"");
    ClassName writer = (ClassName) result.getParamValue("cache-writer");
    assertThat(writer.getClassName()).isEqualTo("my.abc");
    assertThat(writer.getInitProperties()).containsOnlyKeys("k1", "k2").containsEntry("k1",
        "v   1");
  }

  @Test
  public void cacheListenerClassOnly() {
    GfshParseResult result =
        parser.parse("create region --name=region --cache-listener=my.abc,my.def");
    ClassName[] listeners = (ClassName[]) result.getParamValue("cache-listener");
    assertThat(listeners).hasSize(2).contains(new ClassName("my.abc"), new ClassName("my.def"));
  }

  @Test
  public void cacheListenerClassAndProps() {
    String json1 = "{'k1':'v1'}";
    String json2 = "{'k2':'v2'}";
    GfshParseResult result = parser
        .parse("create region --name=region --cache-listener=my.abc" + json1 + ",my.def" + json2);
    ClassName[] listeners = (ClassName[]) result.getParamValue("cache-listener");
    assertThat(listeners).hasSize(2).contains(new ClassName("my.abc", json1),
        new ClassName("my.def", json2));
  }

  @Test
  public void cacheListenerClassAndJsonWithComma() {
    String json1 = "{'k1':'v1','k2':'v2'}";
    String json2 = "{'k2':'v2'}";
    GfshParseResult result = parser
        .parse("create region --name=region --cache-listener=my.abc" + json1 + ",my.def" + json2);
    ClassName[] listeners = (ClassName[]) result.getParamValue("cache-listener");
    assertThat(listeners).hasSize(2).contains(new ClassName("my.abc", json1),
        new ClassName("my.def", json2));
  }

  @Test
  public void cacheListenerClassAndJsonWithCommaAndSpace() {
    String json1 = "{'k1' : 'v1', 'k2' : 'v2'}";
    String json2 = "{'k2' : 'v2'}";
    GfshParseResult result = parser.parse(
        "create region --name=region --cache-listener=\"my.abc" + json1 + ",my.def" + json2 + "\"");
    ClassName[] listeners = (ClassName[]) result.getParamValue("cache-listener");
    assertThat(listeners).hasSize(2).contains(new ClassName("my.abc", json1),
        new ClassName("my.def", json2));
  }

  @Test
  public void invalidCompressor() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --compressor=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains("abc-def is an invalid Compressor.");
  }

  @Test
  public void invalidKeyType() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --key-type=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void invalidValueType() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --value-type=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getMessageFromContent()).contains("Invalid command");
  }

  @Test
  public void statisticsMustBeEnabledForExpiration() {
    parser.executeAndAssertThat(command, COMMAND + "--entry-idle-time-expiration=10")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--entry-time-to-live-expiration=10")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--region-idle-time-expiration=10")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--region-time-to-live-expiration=10")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--entry-idle-time-expiration-action=destroy")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--entry-time-to-live-expiration-action=destroy")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--region-idle-time-expiration-action=destroy")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser
        .executeAndAssertThat(command, COMMAND + "--region-time-to-live-expiration-action=destroy")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--entry-time-to-live-custom-expiry=abc")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");

    parser.executeAndAssertThat(command, COMMAND + "--entry-idle-time-custom-expiry=abc")
        .statusIsError().containsOutput("Statistics must be enabled for expiration");
  }

  @Test
  public void nameCollisionCheck() {
    when(regionMXBean.getMemberCount()).thenReturn(2);
    when(regionMXBean.getEmptyNodes()).thenReturn(1);
    when(regionMXBean.getRegionType()).thenReturn("REPLICATE");
    parser.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Region /region already exists on the cluster");
    parser.executeAndAssertThat(command, COMMAND + " --if-not-exists").statusIsSuccess()
        .containsOutput("Skipping: Region /region already exists on the cluster");
  }
}
