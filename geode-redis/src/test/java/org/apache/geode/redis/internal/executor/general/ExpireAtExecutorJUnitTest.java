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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.executor.ExpireAtExecutor;

public class ExpireAtExecutorJUnitTest {

  private ExecutionHandlerContext context;
  private Command command;
  private UnpooledByteBufAllocator byteBuf;
  Executor subject;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    command = mock(Command.class);
    byteBuf = new UnpooledByteBufAllocator(false);
    subject = new ExpireAtExecutor();
  }

  @Test
  public void calledWithTooFewCommandArguments_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithTooFewArguments);

    subject.executeCommand(command, context);
    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    AssertionsForClassTypes.assertThat(capturedErrors.get(0).toString(defaultCharset()))
        .startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithTooManyCommandArguments_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());
    commandsAsBytesWithTooFewArguments.add("1".getBytes());
    commandsAsBytesWithTooFewArguments.add("extra-argument".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithTooFewArguments);

    subject.executeCommand(command, context);
    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    AssertionsForClassTypes.assertThat(capturedErrors.get(0).toString(defaultCharset()))
        .startsWith("-ERR The wrong number of arguments");
  }

  @Test
  public void calledWithInvalidTimeStamp_returnsError() {
    List<byte[]> commandsAsBytesWithTooFewArguments = new ArrayList<>();
    commandsAsBytesWithTooFewArguments.add("EXPIREAT".getBytes());
    commandsAsBytesWithTooFewArguments.add("key".getBytes());
    commandsAsBytesWithTooFewArguments.add("not-a-timestamp".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytesWithTooFewArguments);

    subject.executeCommand(command, context);
    verify(command, times(1)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    AssertionsForClassTypes.assertThat(capturedErrors.get(0).toString(defaultCharset()))
        .startsWith("-ERR value is not an integer or out of range");
  }
}
