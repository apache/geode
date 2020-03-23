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

package org.apache.geode.redis.internal.executor.string;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;


public class GetRangeExecutorJUnitTest {

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
    Executor getRangeExecutor = new GetRangeExecutor();
    List<byte[]> commandsAsBytesWithThreeArgs = new ArrayList<>();
    commandsAsBytesWithThreeArgs.add("GETRANGE".getBytes());
    commandsAsBytesWithThreeArgs.add("key".getBytes());
    commandsAsBytesWithThreeArgs.add("1".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithThreeArgs);

    getRangeExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void calledWithTooManyCommandArguments_returnsErrorMessage() {
    Executor getRangeExecutor = new GetRangeExecutor();
    List<byte[]> commandsAsBytesWithFiveArgs = new ArrayList<>();
    commandsAsBytesWithFiveArgs.add("GETRANGE".getBytes());
    commandsAsBytesWithFiveArgs.add("key".getBytes());
    commandsAsBytesWithFiveArgs.add("1".getBytes());
    commandsAsBytesWithFiveArgs.add("5".getBytes());
    commandsAsBytesWithFiveArgs.add("avocado".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithFiveArgs);

    getRangeExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void calledWithInvalidArgumentTypeForStartIndex_returnsErrorMessage() {
    Executor getRangeExecutor = new GetRangeExecutor();
    List<byte[]> commandsAsBytesWithInvalidStartIndex = new ArrayList<>();

    commandsAsBytesWithInvalidStartIndex.add("GETRANGE".getBytes());
    commandsAsBytesWithInvalidStartIndex.add("key".getBytes());
    commandsAsBytesWithInvalidStartIndex.add("not-a-number".getBytes());
    commandsAsBytesWithInvalidStartIndex.add("1".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithInvalidStartIndex);

    getRangeExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR value is not an integer or out of range");
  }

  @Test
  public void calledWithInvalidArgumentTypeForEndIndex_returnsErrorMessage() {

    Executor getRangeExecutor = new GetRangeExecutor();
    List<byte[]> commandsAsBytesWithInvalidEndIndex = new ArrayList<>();

    commandsAsBytesWithInvalidEndIndex.add("GETRANGE".getBytes());
    commandsAsBytesWithInvalidEndIndex.add("key".getBytes());
    commandsAsBytesWithInvalidEndIndex.add("1".getBytes());
    commandsAsBytesWithInvalidEndIndex.add("not-a-number".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithInvalidEndIndex);

    getRangeExecutor.executeCommand(command, context);

    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR value is not an integer or out of range");
  }

}
