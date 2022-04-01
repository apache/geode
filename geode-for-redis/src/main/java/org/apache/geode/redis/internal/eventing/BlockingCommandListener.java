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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.executor.RedisResponse;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BlockingCommandListener implements EventListener {

  private final ExecutionHandlerContext context;
  private final Command command;
  private final List<RedisKey> keys;
  private final double timeoutSeconds;
  private final long timeSubmitted;
  private final AtomicBoolean active = new AtomicBoolean(true);

  /**
   * Constructor to create an instance of a BlockingCommandListener in response to a blocking
   * command. When receiving a relevant event, blocking commands simply resubmit the command
   * into the Netty pipeline.
   *
   * @param context the associated ExecutionHandlerContext
   * @param command the blocking command associated with this listener
   * @param keys the list of keys the command is interested in
   * @param timeoutSeconds the timeout for the command to block in seconds
   */
  public BlockingCommandListener(ExecutionHandlerContext context, Command command,
      List<RedisKey> keys, double timeoutSeconds) {
    this.context = context;
    this.command = command;
    this.timeoutSeconds = timeoutSeconds;
    this.keys = Collections.unmodifiableList(keys);
    timeSubmitted = System.nanoTime();
  }

  @Override
  public List<RedisKey> keys() {
    return keys;
  }

  @Override
  public EventResponse process(NotificationEvent notificationEvent, RedisKey key) {
    if (!keys.contains(key)) {
      return EventResponse.CONTINUE;
    }

    resubmitCommand();
    return EventResponse.REMOVE_AND_STOP;
  }

  @Override
  public void resubmitCommand() {
    if (!active.compareAndSet(true, false)) {
      return;
    }

    // Recalculate the timeout since we've already been waiting
    double adjustedTimeoutSeconds = 0;
    if (timeoutSeconds > 0.0D) {
      long timeoutNanos = (long) (timeoutSeconds * 1e9);
      long adjustedTimeoutNanos = timeoutNanos - (System.nanoTime() - timeSubmitted);
      adjustedTimeoutNanos = Math.max(1, adjustedTimeoutNanos);
      adjustedTimeoutSeconds = ((double) adjustedTimeoutNanos) / 1e9;
    }

    // The commands we are currently supporting all have the timeout at the end of the argument
    // list. Some newer Redis 7 commands (BLMPOP and BZMPOP) have the timeout as the first argument
    // after the command. We'll need to adjust this once those commands are supported.
    List<byte[]> commandArguments = command.getCommandArguments();
    commandArguments.set(commandArguments.size() - 1, Coder.doubleToBytes(adjustedTimeoutSeconds));

    context.resubmitCommand(command);
  }

  @Override
  public void scheduleTimeout(ScheduledExecutorService executor, EventDistributor distributor) {
    if (timeoutSeconds == 0 || !active.get()) {
      return;
    }

    long timeoutNanos = (long) (timeoutSeconds * 1e9);
    executor.schedule(() -> timeout(distributor), timeoutNanos, TimeUnit.NANOSECONDS);
  }

  @VisibleForTesting
  void timeout(EventDistributor distributor) {
    if (active.compareAndSet(true, false)) {
      distributor.removeListener(this);
      context.writeToChannel(RedisResponse.nilArray());
    }
  }

  @Override
  public String toString() {
    return "BlockingCommandListener [command:" + command.getCommandType().name()
        + " keys:" + keys + "]";
  }

}
