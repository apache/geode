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

package org.apache.geode.redis.internal.executor.key;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.executor.Executor;
import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class PersistExecutorJUnitTest {

  private ExecutionHandlerContext context;
  private Command command;
  private UnpooledByteBufAllocator byteBuf;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    command = mock(Command.class);
    byteBuf = new UnpooledByteBufAllocator(false);
  }

  @Test
  public void calledWithTooFewCommandArguments_returnsError() {
    Executor executor = new PersistExecutor();
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("PERSIST".getBytes());

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithTooFewArguments);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString()).startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithTooManyCommandArguments_returnsErrorMessage() {
    Executor executor = new PersistExecutor();
    List<byte[]> commandsAsBytesWithTooManyArguments = new ArrayList<>();
    commandsAsBytesWithTooManyArguments.add("PERSIST".getBytes());
    commandsAsBytesWithTooManyArguments.add("key".getBytes());
    commandsAsBytesWithTooManyArguments.add("Bonus!".getBytes());

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithTooManyArguments);

    RedisResponse response = executor.executeCommand(command, context);

    assertThat(response.toString()).startsWith("-ERR The wrong number of arguments");
  }
}
