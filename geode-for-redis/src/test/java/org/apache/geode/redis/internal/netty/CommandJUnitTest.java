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
package org.apache.geode.redis.internal.netty;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;

/**
 * Test case for the command
 */
public class CommandJUnitTest {

  /**
   * Test method for {@link Command#Command(java.util.List)}.
   */
  @Test
  public void testCommand() {
    List<byte[]> list1 = null;
    assertThatThrownBy(() -> new Command(list1))
        .hasMessageContaining("List of command elements cannot be empty");

    List<byte[]> list2 = new ArrayList<>();

    assertThatThrownBy(() -> new Command(list2))
        .hasMessageContaining("List of command elements cannot be empty");

    List<byte[]> list3 = new ArrayList<>();
    list3.add(stringToBytes("Garbage"));

    Command cmd = new Command(list3);
    assertThat(cmd.getCommandType()).isNotNull();

    assertThat(cmd.getCommandType()).isEqualTo(RedisCommandType.UNKNOWN);
    list3.clear();
    list3.add(stringToBytes(RedisCommandType.HEXISTS.toString()));
    cmd = new Command(list3);
    assertThat(cmd.getCommandType()).isNotNull();
    assertThat(cmd.getCommandType()).isEqualTo(RedisCommandType.HEXISTS);
    assertThat(cmd.getProcessedCommand()).isEqualTo(list3);
    assertThat(cmd.getKey()).isNull();

    list3.add(stringToBytes("Arg1"));
    cmd = new Command(list3);
    assertThat(cmd.getKey()).isNotNull();
    assertThat(cmd.getStringKey()).isEqualTo("Arg1");
  }

  @Test
  public void verifyGetCommandArguments() {
    Command cmd = new Command(Collections.singletonList(stringToBytes("cmd")));
    assertThat(cmd.getCommandArguments()).isEmpty();

    cmd = new Command(Arrays.asList(stringToBytes("cmd"), stringToBytes("arg1")));
    assertThat(cmd.getCommandArguments()).containsExactly(stringToBytes("arg1"));

    cmd = new Command(
        Arrays.asList(stringToBytes("cmd"), stringToBytes("arg1"), stringToBytes("arg2")));
    assertThat(cmd.getCommandArguments()).containsExactly(stringToBytes("arg1"),
        stringToBytes("arg2"));

  }

  @Test
  public void toStringForAUTHCommandDoesNotReturnArguments() {
    String password = "password";
    byte[] authBytes = stringToBytes(RedisCommandType.AUTH.name());
    byte[] passwordBytes = stringToBytes(password);

    Command authCommandWithOneArgument = new Command(Arrays.asList(authBytes, passwordBytes));
    assertThat(authCommandWithOneArgument.toString()).doesNotContain(password);

    Command authCommandWithTwoArguments =
        new Command(Arrays.asList(authBytes, passwordBytes, passwordBytes));
    assertThat(authCommandWithTwoArguments.toString()).doesNotContain(password);
  }

  @Test
  public void toStringForAUTHCommandReturnsNumberOfArguments() {
    String password = "password";
    byte[] authBytes = stringToBytes(RedisCommandType.AUTH.name());
    byte[] passwordBytes = stringToBytes(password);

    Command authCommandWithOneArgument = new Command(Arrays.asList(authBytes, passwordBytes));
    assertThat(authCommandWithOneArgument.toString()).contains("AUTH command with 1 argument(s)");

    Command authCommandWithTwoArguments =
        new Command(Arrays.asList(authBytes, passwordBytes, passwordBytes));
    assertThat(authCommandWithTwoArguments.toString()).contains("AUTH command with 2 argument(s)");
  }
}
