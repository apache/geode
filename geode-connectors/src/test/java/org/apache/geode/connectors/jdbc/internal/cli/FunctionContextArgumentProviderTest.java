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
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FunctionContextArgumentProviderTest {

  private FunctionContext<?> context;
  private DistributedMember distributedMember;
  private ResultSender<Object> resultSender;
  private InternalJdbcConnectorService service;

  private FunctionContextArgumentProvider jdbcCommandFunctionContext;

  @Before
  public void setUp() {
    context = mock(FunctionContext.class);
    resultSender = mock(ResultSender.class);
    InternalCache cache = mock(InternalCache.class);
    DistributedSystem system = mock(DistributedSystem.class);
    distributedMember = mock(DistributedMember.class);
    service = mock(InternalJdbcConnectorService.class);

    when(context.getResultSender()).thenReturn(resultSender);
    when(context.getCache()).thenReturn(cache);
    when(cache.getDistributedSystem()).thenReturn(system);
    when(system.getDistributedMember()).thenReturn(distributedMember);
    when(cache.getService(eq(InternalJdbcConnectorService.class))).thenReturn(service);

    jdbcCommandFunctionContext = new FunctionContextArgumentProvider();
  }

  @Test
  public void getMemberReturnsMemberNameInsteadOfId() throws Exception {
    when(distributedMember.getId()).thenReturn("myId");
    when(distributedMember.getName()).thenReturn("myName");

    String member = jdbcCommandFunctionContext.getMember(context);

    assertThat(member).isEqualTo("myName");
  }

  @Test
  public void getMemberReturnsMemberIdIfNameIsMissing() throws Exception {
    when(distributedMember.getId()).thenReturn("myId");

    String member = jdbcCommandFunctionContext.getMember(context);

    assertThat(member).isEqualTo("myId");
  }

}
