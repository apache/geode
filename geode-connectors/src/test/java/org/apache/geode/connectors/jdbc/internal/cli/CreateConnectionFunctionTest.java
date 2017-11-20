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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigBuilder;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class CreateConnectionFunctionTest {

  private CreateConnectionFunction function;
  private FunctionContext<ConnectionConfiguration> context;
  private ResultSender<Object> resultSender;
  private InternalCache cache;
  private DistributedSystem system;
  private DistributedMember member;
  private ConnectionConfiguration configuration;
  private InternalJdbcConnectorService service;

  @Before
  public void setup() {
    configuration = new ConnectionConfigBuilder().build();

    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    cache = mock(InternalCache.class);
    system = mock(DistributedSystem.class);
    member = mock(DistributedMember.class);
    service = mock(InternalJdbcConnectorService.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(member);
    when(context.getArguments()).thenReturn(configuration);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);

    function = new CreateConnectionFunction();
  }

  @Test
  public void isHAIsFalse() throws Exception {
    assertThat(function.isHA()).isFalse();
  }

  @Test
  public void getId() throws Exception {
    assertThat(function.getId()).isEqualTo(function.getClass().getName());
  }

  @Test
  public void execute() throws Exception {
    function.execute(context);
    verify(service, times(1)).createConnectionConfig(configuration);
  }

}
