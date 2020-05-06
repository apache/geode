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

import static org.apache.geode.redis.internal.RedisCommandType.SADD;
import static org.apache.geode.redis.internal.RedisCommandType.SCARD;
import static org.apache.geode.redis.internal.RedisCommandType.SDIFF;
import static org.apache.geode.redis.internal.RedisCommandType.SDIFFSTORE;
import static org.apache.geode.redis.internal.RedisCommandType.SINTERSTORE;
import static org.apache.geode.redis.internal.RedisCommandType.SISMEMBER;
import static org.apache.geode.redis.internal.RedisCommandType.SMEMBERS;
import static org.apache.geode.redis.internal.RedisCommandType.SMOVE;
import static org.apache.geode.redis.internal.RedisCommandType.SPOP;
import static org.apache.geode.redis.internal.RedisCommandType.SREM;
import static org.apache.geode.redis.internal.RedisCommandType.SUNION;
import static org.apache.geode.redis.internal.RedisCommandType.SUNIONSTORE;
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
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SADD".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SADD.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSAdd() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SADD".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SADD.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgsPassedToSCard() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SCARD".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SCARD.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenMoreThanTwoArgsPassedToSCard() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SCARD".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SCARD.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSMembers() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMEMBERS".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMEMBERS.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenMoreThanTwoArgsPassedToSMembers() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMEMBERS".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("key2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMEMBERS.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSIsMember() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SISMEMBER.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSIsMember() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    Command command = new Command(commandsAsBytes);
    commandsAsBytes.add("key1".getBytes());

    Throwable thrown = catchThrowable(() -> SISMEMBER.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenMoreThanThreeArgsPassedToSIsMember() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SISMEMBER".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("member1".getBytes());
    commandsAsBytes.add("member2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SISMEMBER.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgsPassedToSMove() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMOVE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenTwoArgsPassedToSMove() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMOVE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenThreeArgsPassedToSMove() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    commandsAsBytes.add("dest".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMOVE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenMoreThanFourArgsPassedToSMove() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SMOVE".getBytes());
    commandsAsBytes.add("source".getBytes());
    commandsAsBytes.add("dest".getBytes());
    commandsAsBytes.add("field1".getBytes());
    commandsAsBytes.add("field2".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SMOVE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenArgsPassedToSDiff() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFF".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SDIFF.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSDiffStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SDIFFSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSDiffStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SDIFFSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SDIFFSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSInter() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTER".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SINTERSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSInterStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTERSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SINTERSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSInterStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SINTERSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SINTERSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSUnion() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNION".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SUNION.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSUnionStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNIONSTORE".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SUNIONSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenOneArgPassedToSUnionStore() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SUNIONSTORE".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SUNIONSTORE.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenNoArgsPassedToSPop() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SPOP.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenWrongNANPassedToSPop() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("NAN".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SPOP.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("value is not an integer or out of range");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessageWhenMoreTwoArgsPassedToSPop() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SPOP".getBytes());
    commandsAsBytes.add("key1".getBytes());
    commandsAsBytes.add("4".getBytes());
    commandsAsBytes.add("invalid".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SPOP.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessage_WhenNoArgsPassedToSRem() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SREM".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SREM.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }

  @Test
  public void verifyErrorMessage_WhenOneArgPassedToSRem() {
    List<byte[]> commandsAsBytes = new ArrayList<>();
    commandsAsBytes.add("SREM".getBytes());
    commandsAsBytes.add("key1".getBytes());
    Command command = new Command(commandsAsBytes);

    Throwable thrown = catchThrowable(() -> SREM.executeCommand(command, context));

    assertThat(thrown).hasMessageContaining("wrong number of arguments");
    assertThat(thrown).isInstanceOf(RedisParametersMismatchException.class);
  }
}
