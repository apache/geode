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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import org.apache.geode.redis.internal.executor.RedisResponse;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class ExpireAtExecutorJUnitTest {

  @Test
  public void calledWithTooFewCommandArguments_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());
    Command command = new Command(commandsAsBytesWithTooFewArguments);

    RedisResponse response =
        new ExpireAtExecutor().executeCommandWithResponse(command, mockContext());

    assertThat(response.toString()).startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithTooManyCommandArguments_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());
    commandsAsBytesWithTooFewArguments.add("1".getBytes());
    commandsAsBytesWithTooFewArguments.add("extra-argument".getBytes());
    Command command = new Command(commandsAsBytesWithTooFewArguments);

    RedisResponse response =
        new ExpireAtExecutor().executeCommandWithResponse(command, mockContext());

    assertThat(response.toString()).startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithInvalidTimeStamp_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());
    commandsAsBytesWithTooFewArguments.add("not-a-timestamp".getBytes());
    Command command = new Command(commandsAsBytesWithTooFewArguments);

    RedisResponse response =
        new ExpireAtExecutor().executeCommandWithResponse(command, mockContext());

    assertThat(response.toString()).startsWith("-ERR value is not an integer or out of range");
  }

  public ExecutionHandlerContext mockContext() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    return context;
  }
}
