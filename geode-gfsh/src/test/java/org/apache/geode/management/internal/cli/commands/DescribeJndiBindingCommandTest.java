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
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.junit.rules.GfshParserRule;

public class DescribeJndiBindingCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  InternalConfigurationPersistenceService ccService;
  CacheConfig cacheConfig;

  private DescribeJndiBindingCommand command;
  JndiBindingsType.JndiBinding binding;
  List<JndiBindingsType.JndiBinding> bindings;

  private static final String COMMAND = "describe jndi-binding ";

  @Before
  public void setUp() throws Exception {
    binding = new JndiBindingsType.JndiBinding();
    binding.setJndiName("jndi-name");
    binding.setType("SIMPLE");
    binding.setJdbcDriverClass("org.postgresql.Driver");
    binding.setConnectionUrl("jdbc:postgresql://localhost:5432/my_db");
    binding.setUserName("MyUser");
    bindings = new ArrayList<>();

    command = spy(DescribeJndiBindingCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    cacheConfig = mock(CacheConfig.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
    when(cacheConfig.getJndiBindings()).thenReturn(bindings);
  }

  @Test
  public void describeJndiBinding() {
    bindings.add(binding);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=jndi-name").statusIsSuccess()
        .hasTableSection()
        .hasColumn("Value")
        .containsExactlyInAnyOrder("SIMPLE", "jndi-name", "org.postgresql.Driver", "MyUser",
            "jdbc:postgresql://localhost:5432/my_db", "", "", "", "", "");
  }

  @Test
  public void bindingEmpty() {
    bindings.clear();
    gfsh.executeAndAssertThat(command, COMMAND + " --name=jndi-name").statusIsError()
        .containsOutput("jndi-name not found");
  }

  @Test
  public void bindingNotFound() {
    bindings.add(binding);
    gfsh.executeAndAssertThat(command, COMMAND + " --name=bad-name").statusIsError()
        .containsOutput("bad-name not found");
  }

}
