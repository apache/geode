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

package org.apache.geode.management.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.commands.ShutdownCommand;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

public class ShutdownCommandInterceptorTest {

  private ShutdownCommand.ShutdownCommandInterceptor interceptor;

  @Before
  public void before() throws Exception {
    interceptor = spy(ShutdownCommand.ShutdownCommandInterceptor.class);
  }

  @Test
  public void whenRespondNo() {
    doReturn(AbstractCliAroundInterceptor.Response.NO).when(interceptor).readYesNo(any(), any());
    ResultModel resultModel = interceptor.preExecution(null);
    assertThat(resultModel.getStatus()).isEqualTo(Result.Status.ERROR);
  }
}
