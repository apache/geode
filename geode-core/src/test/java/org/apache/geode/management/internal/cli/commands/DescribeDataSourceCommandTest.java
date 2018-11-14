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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.commands.CreateJndiBindingCommand.DATASOURCE_TYPE;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeDataSourceCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private DescribeDataSourceCommand command;
  private InternalCache cache;
  JndiBindingsType.JndiBinding binding;
  List<JndiBindingsType.JndiBinding> bindings;

  private static String COMMAND = "describe data-source";

  @Before
  public void setUp() throws Exception {
    cache = mock(InternalCache.class);
    when(cache.getDistributionManager()).thenReturn(mock(DistributionManager.class));
    command = spy(DescribeDataSourceCommand.class);
    command.setCache(cache);

    binding = new JndiBindingsType.JndiBinding();
    binding.setJndiName("name");
    binding.setType(DATASOURCE_TYPE.POOLED.getType());
    bindings = new ArrayList<>();
    InternalConfigurationPersistenceService clusterConfigService =
        mock(InternalConfigurationPersistenceService.class);
    CacheConfig cacheConfig = mock(CacheConfig.class);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
    bindings.add(binding);

    doReturn(clusterConfigService).when(command).getConfigurationPersistenceService();
    doReturn(cacheConfig).when(clusterConfigService).getCacheConfig(any());
  }

  @Test
  public void missingMandatory() {
    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Invalid command: describe data-source");
  }

  @Test
  public void nameWorks() {
    gfsh.executeAndAssertThat(command, COMMAND + " --name=name").statusIsSuccess();
  }

}
