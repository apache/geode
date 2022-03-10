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

import java.util.Collections;
import java.util.List;

import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BlockingCommandListener implements EventListener {

  private final ExecutionHandlerContext context;
  private final RedisCommandType command;
  private final List<RedisKey> keys;
  private final List<byte[]> commandArgs;
  private final long timeoutNanos;
  private final long timeSubmitted;
  private Runnable cleanupTask;

  /**
   * Constructor to create an instance of a BlockingCommandListener in response to a blocking
   * command. When receiving a relevant event, blocking commands simply resubmit the command
   * into the Netty pipeline.
   *
   * @param context the associated ExecutionHandlerContext
   * @param command the blocking command associated with this listener
   * @param keys the list of keys the command is interested in
   * @param timeoutSeconds the timeout for the command to block in seconds
   * @param commandArgs all arguments to the command which are used for resubmission
   */
  public BlockingCommandListener(ExecutionHandlerContext context, RedisCommandType command,
      List<RedisKey> keys, double timeoutSeconds, List<byte[]> commandArgs) {
    this.context = context;
    this.command = command;
    this.timeoutNanos = (long) (timeoutSeconds * 1e9);
    this.keys = Collections.unmodifiableList(keys);
    this.commandArgs = commandArgs;
    timeSubmitted = System.nanoTime();
  }

  @Override
  public List<RedisKey> keys() {
    return keys;
  }

  @Override
  public EventResponse process(RedisCommandType commandType, RedisKey key) {
    if (!keys.contains(key)) {
      return EventResponse.CONTINUE;
    }

    resubmitCommand();
    return EventResponse.REMOVE_AND_STOP;
  }

  @Override
  public void resubmitCommand() {
    // Recalculate the timeout since we've already been waiting
    double adjustedTimeoutSeconds = 0;
    if (timeoutNanos > 0) {
      long adjustedTimeoutNanos = timeoutNanos - (System.nanoTime() - timeSubmitted);
      adjustedTimeoutNanos = Math.max(1, adjustedTimeoutNanos);
      adjustedTimeoutSeconds = ((double) adjustedTimeoutNanos) / 1e9;
    }

    // The commands we are currently supporting all have the timeout at the end of the argument
    // list. Some newer Redis 7 commands (BLMPOP and BZMPOP) have the timeout as the first argument
    // after the command. We'll need to adjust this once those commands are supported.
    commandArgs.set(commandArgs.size() - 1, Coder.doubleToBytes(adjustedTimeoutSeconds));

    context.resubmitCommand(new Command(command, commandArgs));
  }

  @Override
  public long getTimeout() {
    return timeoutNanos;
  }

  @Override
  public void timeout() {
    context.writeToChannel(RedisResponse.nilArray());
  }

  @Override
  public void setCleanupTask(Runnable cleanupTask) {
    this.cleanupTask = cleanupTask;
  }

  @Override
  public void cleanup() {
    if (cleanupTask != null) {
      cleanupTask.run();
    }
  }

  @Override
  public String toString() {
    return "BlockingCommandListener [command:" + command.name() + " keys:" + keys + "]";
  }

}
