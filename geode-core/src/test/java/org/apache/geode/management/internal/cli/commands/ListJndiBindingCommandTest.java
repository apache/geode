/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.cli.commands;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.test.junit.rules.GfshParserRule;


public class ListJndiBindingCommandTest {
  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private ListJndiBindingCommand command;
  private InternalConfigurationPersistenceService ccService;
  private CacheConfig cacheConfig;


  @Before
  public void setUp() throws Exception {
    command = spy(ListJndiBindingCommand.class);
    ccService = mock(InternalConfigurationPersistenceService.class);
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    cacheConfig = mock(CacheConfig.class);
    when(ccService.getCacheConfig("cluster")).thenReturn(cacheConfig);
  }

  @Test
  public void noServiceNoMember() {
    doReturn(null).when(command).getConfigurationPersistenceService();
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);
    gfsh.executeAndAssertThat(command, "list jndi-binding").statusIsError()
        .containsOutput("No members found");
  }

  @Test
  public void hasServiceNoBindingNoMember() {
    doReturn(ccService).when(command).getConfigurationPersistenceService();
    when(cacheConfig.getJndiBindings()).thenReturn(Collections.emptyList());
    doReturn(Collections.emptySet()).when(command).findMembers(null, null);
    gfsh.executeAndAssertThat(command, "list jndi-binding").statusIsSuccess()
        .containsOutput("No JNDI-bindings found in cluster configuration")
        .containsOutput("No members found");
  }
}
