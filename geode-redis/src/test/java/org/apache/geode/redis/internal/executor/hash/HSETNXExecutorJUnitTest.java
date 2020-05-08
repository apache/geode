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

package org.apache.geode.redis.internal.executor.hash;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;

public class HSETNXExecutorJUnitTest {
  ExecutionHandlerContext context;
  UnpooledByteBufAllocator byteBuf;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
  }

  @Test
  public void verifyErrorMessageWhenOneArg() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("HSETNX".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassed() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("HSETNX".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenThreeArgsPassedToSAdd() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("HSETNX".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("field1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenFiveArgsPassedToSAdd() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("HSETNX".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("field1".getBytes());
    commandsAsBytes.add("value1".getBytes());
    commandsAsBytes.add("field2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenSixArgsPassedToSAdd() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("HSETNX".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("field1".getBytes());
    commandsAsBytes.add("value1".getBytes());
    commandsAsBytes.add("field2".getBytes());
    commandsAsBytes.add("value2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> command.execute(context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }
}
