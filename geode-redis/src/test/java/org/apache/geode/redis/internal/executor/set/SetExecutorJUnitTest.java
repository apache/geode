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

package org.apache.geode.redis.internal.executor.set;

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

public class SetExecutorJUnitTest {
  ExecutionHandlerContext context;
  Command command;
  UnpooledByteBufAllocator byteBuf;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    command = mock(Command.class);
    byteBuf = new UnpooledByteBufAllocator(false);
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSAdd() {
    Executor sAddExecutor = new SAddExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SADD".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sAddExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    sAddExecutor.executeCommand(command, context);

    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSCard() {
    Executor sCardExecutor = new SCardExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SCARD".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sCardExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    sCardExecutor.executeCommand(command, context);
    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSMembers() {
    Executor sMembersExecutor = new SMembersExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMEMBERS".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sMembersExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    sMembersExecutor.executeCommand(command, context);
    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSIsMember() {
    Executor sIsMemberExecutor = new SIsMemberExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sIsMemberExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    sIsMemberExecutor.executeCommand(command, context);

    commandsAsBytes.add("member1".getBytes());
    commandsAsBytes.add("member2".getBytes());
    sIsMemberExecutor.executeCommand(command, context);

    verify(command, times(3)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(2).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSMove() {
    Executor sMoveExecutor = new SMoveExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sMoveExecutor.executeCommand(command, context);

    commandsAsBytes.add("source".getBytes());
    sMoveExecutor.executeCommand(command, context);

    commandsAsBytes.add("dest".getBytes());
    sMoveExecutor.executeCommand(command, context);

    commandsAsBytes.add("field1".getBytes());
    commandsAsBytes.add("field2".getBytes());
    sMoveExecutor.executeCommand(command, context);

    verify(command, times(4)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(2).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(3).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSDiff() {
    Executor sdiffExecutor = new SDiffExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFF".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sdiffExecutor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    assertThat(argsErrorCaptor.getValue().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSDiffStore() {
    Executor sDiffStoreExecutor = new SDiffStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sDiffStoreExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    sDiffStoreExecutor.executeCommand(command, context);

    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSInter() {
    Executor sInterExecutor = new SInterExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTER".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sInterExecutor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    assertThat(argsErrorCaptor.getValue().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSInterStore() {
    Executor sInterStoreExecutor = new SInterStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTERSTORE".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sInterStoreExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    sInterStoreExecutor.executeCommand(command, context);

    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSUnion() {
    Executor sUnionExecutor = new SUnionExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNION".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sUnionExecutor.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    assertThat(argsErrorCaptor.getValue().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSUnionStore() {
    Executor sUnionStoreExecutor = new SUnionStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNIONSTORE".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sUnionStoreExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    sUnionStoreExecutor.executeCommand(command, context);

    verify(command, times(2)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongArgsPassedToSPop() {
    Executor sPopExecutor = new SPopExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    ArgumentCaptor<ByteBuf> argsErrorCaptor = ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);
    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    sPopExecutor.executeCommand(command, context);

    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("NAN".getBytes());
    sPopExecutor.executeCommand(command, context);

    commandsAsBytes.set(2, "4".getBytes()); // switch "NAN" to a real number
    commandsAsBytes.add("invalid".getBytes());
    sPopExecutor.executeCommand(command, context);

    verify(command, times(3)).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();
    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(1).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
    assertThat(capturedErrors.get(2).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessage_WhenTooFewArgsPassedToSRem() {
    Executor subject = new SRemExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SREM".getBytes());

    ArgumentCaptor<ByteBuf> argsErrorCaptor =
        ArgumentCaptor.forClass(ByteBuf.class);

    when(context.getByteBufAllocator()).thenReturn(byteBuf);

    when(command.getProcessedCommand()).thenReturn(commandsAsBytes);

    commandsAsBytes.add("key1".getBytes());
    subject.executeCommand(command, context);

    verify(command).setResponse(argsErrorCaptor.capture());

    List<ByteBuf> capturedErrors = argsErrorCaptor.getAllValues();

    assertThat(capturedErrors.get(0).toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
