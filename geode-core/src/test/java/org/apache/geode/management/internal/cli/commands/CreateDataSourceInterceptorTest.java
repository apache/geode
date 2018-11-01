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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.shell.Gfsh;

public class CreateDataSourceInterceptorTest {
  private Gfsh gfsh;
  private GfshParseResult parseResult;
  private CreateDataSourceInterceptor interceptor;

  @Before
  public void setUp() throws Exception {
    gfsh = mock(Gfsh.class);
    parseResult = mock(GfshParseResult.class);
    interceptor = new CreateDataSourceInterceptor(gfsh);
  }

  @Test
  public void defaultsAreOk() {

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void nonPooledIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("false");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void nonPooledWithEmptyPoolPropertiesIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("false");
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOL_PROPERTIES)).thenReturn("");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void nonPooledWithEmptyPoolFactoryIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("false");
    when(
        parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS))
            .thenReturn("");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void pooledIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("true");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void pooledWithPoolPropertiesIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("true");
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOL_PROPERTIES))
        .thenReturn("pool properties value");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void pooledWithPoolFactoryIsOk() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("true");
    when(
        parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS))
            .thenReturn("pool factory value");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.OK);
  }

  @Test
  public void nonPooledWithPoolPropertiesIsError() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("false");
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOL_PROPERTIES))
        .thenReturn("pool properties value");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.getInfoSection("info").getContent().get(0))
        .contains(CreateDataSourceInterceptor.POOL_PROPERTIES_ONLY_VALID_ON_POOLED_DATA_SOURCE);
  }

  @Test
  public void nonPooledWithPoolFactoryIsError() {
    when(parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED)).thenReturn("false");
    when(
        parseResult.getParamValueAsString(CreateDataSourceCommand.POOLED_DATA_SOURCE_FACTORY_CLASS))
            .thenReturn("pool factory value");

    ResultModel result = interceptor.preExecution(parseResult);

    assertThat(result.getStatus()).isEqualTo(Status.ERROR);
    assertThat(result.getInfoSection("info").getContent().get(0))
        .contains(
            CreateDataSourceInterceptor.POOLED_DATA_SOURCE_FACTORY_CLASS_ONLY_VALID_ON_POOLED_DATA_SOURCE);
  }
}
