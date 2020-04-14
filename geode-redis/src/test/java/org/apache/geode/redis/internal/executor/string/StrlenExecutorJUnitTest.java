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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;

public class StrlenExecutorJUnitTest {
  private ExecutionHandlerContext context;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    UnpooledByteBufAllocator byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
  }

  @Test
  public void calledWithTooFewOptions_returnsError() {
    Executor strlenExecutor = new StrlenExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("STRLEN".getBytes());
    Command command = new Command(commandsAsBytes);

    strlenExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void calledWithTooManyOptions_returnsError() {
    Executor strlenExecutor = new StrlenExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("APPEND".getBytes());
    commandsAsBytes.add("key".getBytes());
    commandsAsBytes.add("BONUS!".getBytes());
    Command command = new Command(commandsAsBytes);

    strlenExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
