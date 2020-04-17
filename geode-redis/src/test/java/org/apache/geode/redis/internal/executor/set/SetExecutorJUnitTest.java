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

public class SetExecutorJUnitTest {
  ExecutionHandlerContext context;
  UnpooledByteBufAllocator byteBuf;

  @Before
  public void setUp() {
    context = mock(ExecutionHandlerContext.class);
    byteBuf = new UnpooledByteBufAllocator(false);
    when(context.getByteBufAllocator()).thenReturn(byteBuf);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSAdd() {
    Executor sAddExecutor = new SAddExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SADD".getBytes());
    Command command = new Command(commandsAsBytes);

    sAddExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSAdd() {
    Executor sAddExecutor = new SAddExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SADD".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    sAddExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgsPassedToSCard() {
    Executor sCardExecutor = new SCardExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SCARD".getBytes());
    Command command = new Command(commandsAsBytes);

    sCardExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenMoreThanTwoArgsPassedToSCard() {
    Executor sCardExecutor = new SCardExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SCARD".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    Command command = new Command(commandsAsBytes);

    sCardExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSMembers() {
    Executor sMembersExecutor = new SMembersExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMEMBERS".getBytes());
    Command command = new Command(commandsAsBytes);

    sMembersExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenMoreThanTwoArgsPassedToSMembers() {
    Executor sMembersExecutor = new SMembersExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMEMBERS".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    Command command = new Command(commandsAsBytes);

    sMembersExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSIsMember() {
    Executor sIsMemberExecutor = new SIsMemberExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    Command command = new Command(commandsAsBytes);

    sIsMemberExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSIsMember() {
    Executor sIsMemberExecutor = new SIsMemberExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    Command command = new Command(commandsAsBytes);
    commandsAsBytes.add("key1".getBytes());

    sIsMemberExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenMoreThanThreeArgsPassedToSIsMember() {
    Executor sIsMemberExecutor = new SIsMemberExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("member1".getBytes());
    commandsAsBytes.add("member2".getBytes());
    Command command = new Command(commandsAsBytes);

    sIsMemberExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgsPassedToSMove() {
    Executor sMoveExecutor = new SMoveExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    Command command = new Command(commandsAsBytes);

    sMoveExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSMove() {
    Executor sMoveExecutor = new SMoveExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    Command command = new Command(commandsAsBytes);

    sMoveExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenThreeArgsPassedToSMove() {
    Executor sMoveExecutor = new SMoveExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    commandsAsBytes.add("dest".getBytes());
    Command command = new Command(commandsAsBytes);

    sMoveExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenMoreThanFourArgsPassedToSMove() {
    Executor sMoveExecutor = new SMoveExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    commandsAsBytes.add("dest".getBytes());
    commandsAsBytes.add("field1".getBytes());
    commandsAsBytes.add("field2".getBytes());
    Command command = new Command(commandsAsBytes);

    sMoveExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenArgsPassedToSDiff() {
    Executor sdiffExecutor = new SDiffExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFF".getBytes());
    Command command = new Command(commandsAsBytes);

    sdiffExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSDiffStore() {
    Executor sDiffStoreExecutor = new SDiffStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    sDiffStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSDiffStore() {
    Executor sDiffStoreExecutor = new SDiffStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    sDiffStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSInter() {
    Executor sInterExecutor = new SInterExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTER".getBytes());
    Command command = new Command(commandsAsBytes);

    sInterExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSInterStore() {
    Executor sInterStoreExecutor = new SInterStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTERSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    sInterStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSInterStore() {
    Executor sInterStoreExecutor = new SInterStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTERSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    sInterStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSUnion() {
    Executor sUnionExecutor = new SUnionExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNION".getBytes());
    Command command = new Command(commandsAsBytes);

    sUnionExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSUnionStore() {
    Executor sUnionStoreExecutor = new SUnionStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNIONSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    sUnionStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSUnionStore() {
    Executor sUnionStoreExecutor = new SUnionStoreExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNIONSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    sUnionStoreExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSPop() {
    Executor sPopExecutor = new SPopExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    Command command = new Command(commandsAsBytes);

    sPopExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenWrongNANPassedToSPop() {
    Executor sPopExecutor = new SPopExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("NAN".getBytes());
    Command command = new Command(commandsAsBytes);

    sPopExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessageWhenMoreTwoArgsPassedToSPop() {
    Executor sPopExecutor = new SPopExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("4".getBytes()); // switch "NAN" to a real number
    commandsAsBytes.add("invalid".getBytes());
    Command command = new Command(commandsAsBytes);

    sPopExecutor.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessage_WhenNoArgsPassedToSRem() {
    Executor subject = new SRemExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SREM".getBytes());
    Command command = new Command(commandsAsBytes);

    subject.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }

  @Test
  public void verifyErrorMessage_WhenOneArgPassedToSRem() {
    Executor subject = new SRemExecutor();
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SREM".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    subject.executeCommand(command, context);

    assertThat(command.getResponse().toString(Charset.defaultCharset()))
        .startsWith("-ERR The wrong number of arguments or syntax was provided");
  }
}
