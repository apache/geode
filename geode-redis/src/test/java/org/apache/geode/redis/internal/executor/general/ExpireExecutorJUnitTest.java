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

package org.apache.geode.redis.internal.executor.general;

import static java.nio.charset.Charset.defaultCharset;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.executor.ExpireExecutor;

public class ExpireExecutorJUnitTest {

  @Test
  public void calledWithTooFewCommandArguments_returnsError() {
    Executor executor = new ExpireExecutor();
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIRE".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());

    Command command = new Command(commandsAsBytesWithTooFewArguments);
    executor.executeCommand(command, mockContext());

    assertThat(command.getResponse().toString(defaultCharset()))
        .startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithTooManyCommandArguments_returnsErrorMessage() {
    Executor executor = new ExpireExecutor();
    List<byte[]> commandsAsBytesWithTooManyArguments = new ArrayList<>();
    commandsAsBytesWithTooManyArguments.add("EXPIRE".getBytes());
    commandsAsBytesWithTooManyArguments.add("key".getBytes());
    commandsAsBytesWithTooManyArguments.add("100".getBytes());
    commandsAsBytesWithTooManyArguments.add("Bonus!".getBytes());
    Command command = new Command(commandsAsBytesWithTooManyArguments);

    executor.executeCommand(command, mockContext());

    assertThat(command.getResponse().toString(defaultCharset()))
        .startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithInvalidCommandArguments_returnsErrorMessage() {
    Executor executor = new ExpireExecutor();
    List<byte[]> commandsAsBytesWithTooManyArguments = new ArrayList<>();
    commandsAsBytesWithTooManyArguments.add("EXPIRE".getBytes());
    commandsAsBytesWithTooManyArguments.add("key".getBytes());
    commandsAsBytesWithTooManyArguments.add("not a number".getBytes());
    Command command = new Command(commandsAsBytesWithTooManyArguments);

    executor.executeCommand(command, mockContext());

    assertThat(command.getResponse().toString(defaultCharset()))
        .startsWith("-ERR value is not an integer or out of range");
  }

  public ExecutionHandlerContext mockContext() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    return context;
  }
}
