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
 *
 */

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

@SuppressWarnings("unchecked")
public class StringGetExecutorJUnitTest {

  private ExecutionHandlerContext context;

  @Before
  public void setup() {
    context = mock(ExecutionHandlerContext.class);
  }

  @Test
  public void testGET_withIncorrectArgumentsReturnsError() {
    List<byte[]> commandArgumentWithTooFewArgs = Arrays.asList(
        "GET".getBytes());

    Command command = new Command(commandArgumentWithTooFewArgs);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments for 'get' command");
  }

  @Test
  public void testGET_withTooManyArgumentsReturnsError() {
    List<byte[]> commandArgumentWithEXNoParameter = Arrays.asList(
        "GET".getBytes(),
        "key".getBytes(),
        "something".getBytes(),
        "somethingelse".getBytes());
    Command command = new Command(commandArgumentWithEXNoParameter);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments for 'get' command");
  }
}
