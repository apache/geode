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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import org.apache.geode.redis.internal.ParameterRequirements.RedisParametersMismatchException;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;


public class KeysExecutorJUnitTest {

  @Test
  public void calledWithTooFewOptions_returnsError() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("KEYS".getBytes());
    Command command = new Command(commandsAsBytes);

    assertThatThrownBy(() -> command.execute(mockContext()))
        .hasMessageContaining("wrong number of arguments")
        .isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void calledWithTooManyOptions_returnsError() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("KEYS".getBytes());
    commandsAsBytes.add("*".getBytes());
    commandsAsBytes.add("**".getBytes());
    Command command = new Command(commandsAsBytes);

    assertThatThrownBy(() -> command.execute(mockContext()))
        .hasMessageContaining("wrong number of arguments")
        .isInstanceOf(RedisParametersMismatchException.class);
  }

  public ExecutionHandlerContext mockContext() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    return context;
  }
}
