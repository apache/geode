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
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class CreateRegionCommandTest {
  @Rule
  public GfshParserRule parser = new GfshParserRule();

  private CreateRegionCommand command;
  private InternalCache cache;

  @Before
  public void before() throws Exception {
    command = spy(CreateRegionCommand.class);
    cache = mock(InternalCache.class);
    doReturn(cache).when(command).getCache();
  }

  @Test
  public void testRegionExistsReturnsCorrectValue() throws Exception {
    assertThat(command.regionExists(cache, null)).isFalse();
  }

  @Test
  public void missingName() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command, "create region");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");
  }

  @Test
  public void missingBothTypeAndUseAttributeFrom() throws Exception {
    CommandResult result =
        parser.executeCommandWithInstance(command, "create region --name=region");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("One of \\\"type\\\" or \\\"template-region\\\" is required.");
  }

  @Test
  public void haveBothTypeAndUseAttributeFrom() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --template-region=regionB");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("Only one of type & template-region can be specified.");
  }

  @Test
  public void invalidEvictionAction() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-action=invalidAction");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("eviction-action must be 'local-destroy' or 'overflow-to-disk'");
  }

  @Test
  public void invalidEvictionAttributes() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-max-memory=1000 --eviction-entry-count=200");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("eviction-max-memory and eviction-entry-count cannot both be specified.");
  }

  @Test
  public void missingEvictionAction() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-max-memory=1000");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("eviction-action must be specified.");
  }

  @Test
  public void invalidEvictionSizerAndCount() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --eviction-entry-count=1 --eviction-object-sizer=abc --eviction-action=local-destroy");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("eviction-object-sizer cannot be specified with eviction-entry-count");
  }

  @Test
  public void templateRegionAttributesNotAvailable() throws Exception {
    doReturn(null).when(command).getRegionAttributes(eq(cache), any());
    doReturn(Collections.emptySet()).when(command).findMembers(any(), any());
    doReturn(true).when(command).regionExists(eq(cache), any());

    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --template-region=regionA");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("Could not retrieve region attributes for given path");
  }

  @Test
  public void defaultValues() throws Exception {
    ResultCollector resultCollector = mock(ResultCollector.class);
    doReturn(resultCollector).when(command).executeFunction(any(), any(), any(Set.class));
    when(resultCollector.getResult()).thenReturn(Collections.emptyList());
    DistributedSystemMXBean dsMBean = mock(DistributedSystemMXBean.class);
    doReturn(dsMBean).when(command).getDSMBean(any());
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any(),
        any());
    doReturn(true).when(command).verifyDistributedRegionMbean(any(), any());

    parser.executeCommandWithInstance(command, "create region --name=A --type=REPLICATE");
    ArgumentCaptor<RegionFunctionArgs> argsCaptor =
        ArgumentCaptor.forClass(RegionFunctionArgs.class);
    verify(command).executeFunction(any(), argsCaptor.capture(), any(Set.class));
    RegionFunctionArgs args = argsCaptor.getValue();

    assertThat(args.getRegionPath()).isEqualTo("/A");
    assertThat(args.getRegionShortcut()).isEqualTo(RegionShortcut.REPLICATE);
    assertThat(args.getTemplateRegion()).isNull();
    assertThat(args.isIfNotExists()).isFalse();
    assertThat(args.getKeyConstraint()).isNull();
    assertThat(args.getValueConstraint()).isNull();
    assertThat(args.isStatisticsEnabled()).isNull();

    assertThat(args.getEntryExpirationIdleTime()).isNull();
    assertThat(args.getEntryExpirationTTL()).isNull();
    assertThat(args.getRegionExpirationIdleTime()).isNull();
    assertThat(args.getRegionExpirationTTL()).isNull();

    assertThat(args.getDiskStore()).isNull();
    assertThat(args.isDiskSynchronous()).isNull();
    assertThat(args.isEnableAsyncConflation()).isNull();
    assertThat(args.isEnableSubscriptionConflation()).isNull();
    assertThat(args.getCacheListeners()).isEmpty();
    assertThat(args.getCacheLoader()).isNull();
    assertThat(args.getCacheWriter()).isNull();
    assertThat(args.getAsyncEventQueueIds()).isEmpty();
    assertThat(args.getGatewaySenderIds()).isEmpty();
    assertThat(args.isConcurrencyChecksEnabled()).isNull();
    assertThat(args.isCloningEnabled()).isNull();
    assertThat(args.isMcastEnabled()).isNull();
    assertThat(args.getConcurrencyLevel()).isNull();
    assertThat(args.getPartitionArgs()).isNotNull();
    assertThat(args.getEvictionMax()).isNull();
    assertThat(args.getCompressor()).isNull();
    assertThat(args.isOffHeap()).isNull();
    assertThat(args.getRegionAttributes()).isNull();
  }

  @Test
  public void invalidCacheListener() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --cache-listener=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("Specify a valid class name for cache-listener.");
  }

  @Test
  public void invalidCacheLoader() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --cache-loader=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("Specify a valid class name for cache-loader.");
  }

  @Test
  public void invalidCacheWriter() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --cache-writer=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString())
        .contains("Specify a valid class name for cache-writer.");
  }

  @Test
  public void invalidCompressor() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --compressor=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("abc-def is an invalid Compressor.");
  }

  @Test
  public void invalidKeyType() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --key-type=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");
  }

  @Test
  public void invalidValueType() throws Exception {
    CommandResult result = parser.executeCommandWithInstance(command,
        "create region --name=region --type=REPLICATE --value-type=abc-def");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.getContent().toString()).contains("Invalid command");
  }
}
