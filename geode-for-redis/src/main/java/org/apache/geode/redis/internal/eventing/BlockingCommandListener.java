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

package org.apache.geode.redis.internal.eventing;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BlockingCommandListener implements EventListener {

  private final ExecutionHandlerContext context;
  private final RedisCommandType command;
  private final List<RedisKey> keys;
  private final byte[][] commandOptions;
  private final long timeout;

  /**
   * Constructor to create an instance of a BlockingCommandListener in response to a blocking
   * command. When receiving a relevant event, blocking commands simply resubmit the command
   * into the Netty pipeline.
   *
   * @param context the associated ExecutionHandlerContext
   * @param command the blocking command
   * @param timeout the timeout for the command to block
   * @param keys the list of keys the command is interested in
   * @param commandOptions any additional options (other than the keys and timeout) required to
   *        resubmit the command.
   */
  public BlockingCommandListener(ExecutionHandlerContext context, RedisCommandType command,
      long timeout, List<RedisKey> keys, byte[]... commandOptions) {
    this.context = context;
    this.command = command;
    this.timeout = timeout;
    this.keys = Collections.unmodifiableList(keys);
    this.commandOptions = commandOptions;
  }

  @Override
  public List<RedisKey> keys() {
    return keys;
  }

  @Override
  public EventResponse process(RedisCommandType commandType, RedisKey key) {
    if (!keys.contains(key)) {
      return EventResponse.CONTIINUE;
    }

    resubmitCommand();
    return EventResponse.REMOVE_AND_STOP;
  }

  @Override
  public void resubmitCommand() {
    List<byte[]> byteArgs = new ArrayList<>(keys.size() + 1);
    byteArgs.add(command.name().getBytes());

    keys.forEach(x -> byteArgs.add(x.toBytes()));
    byteArgs.addAll(Arrays.asList(commandOptions));

    // The commands we are currently supporting all have the timeout at the end of the argument
    // list. Some newer Redis 7 commands (BLMPOP and BZMPOP) have the timeout as the first argument
    // after the command. We'll need to adjust this once those commands are supported.
    byteArgs.add(Coder.longToBytes(timeout));

    context.resubmitCommand(new Command(command, byteArgs));
  }

  @Override
  public long getTimeout() {
    return timeout;
  }

  @Override
  public String toString() {
    return "BlockingCommandListener [command:" + command.name() + " keys:" + keys + "]";
  }

}
