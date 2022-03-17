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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.commands.Command;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.netty.Coder;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class BlockingCommandListenerTest {

  @Test
  public void testTimeoutIsAdjusted() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    List<byte[]> commandArgs = Arrays.asList("KEY".getBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Collections.emptyList(), 1.0D);

    listener.resubmitCommand();

    ArgumentCaptor<Command> argumentCaptor = ArgumentCaptor.forClass(Command.class);
    verify(context, times(1)).resubmitCommand(argumentCaptor.capture());

    double timeout = Coder.bytesToDouble(argumentCaptor.getValue().getCommandArguments().get(0));
    assertThat(timeout).isLessThan(1.0D);
  }

  @Test
  public void testAdjustedTimeoutDoesNotBecomeNegative() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    List<byte[]> commandArgs = Arrays.asList("KEY".getBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Collections.emptyList(), 1e-9);

    listener.resubmitCommand();

    ArgumentCaptor<Command> argumentCaptor = ArgumentCaptor.forClass(Command.class);
    verify(context, times(1)).resubmitCommand(argumentCaptor.capture());

    double timeout = Coder.bytesToDouble(argumentCaptor.getValue().getCommandArguments().get(0));
    assertThat(timeout).isEqualTo(1e-9);
  }

  @Test
  public void testListenerIsRemovedAfterTimeout() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    List<byte[]> commandArgs = Arrays.asList("KEY".getBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Collections.emptyList(), 1e-9);
    EventDistributor eventDistributor = new EventDistributor();

    eventDistributor.registerListener(listener);

    await().atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(eventDistributor.getRegisteredKeys()).isEqualTo(0));
  }

  @Test
  public void testListenersAffectedByBucketMovementAreInactiveAndDoNotProcessTimeout() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    RedisKey key = new RedisKey("KEY".getBytes());
    List<byte[]> commandArgs = Arrays.asList(key.toBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Collections.singletonList(key), 1);
    EventDistributor eventDistributor = new EventDistributor();

    eventDistributor.registerListener(listener);

    eventDistributor.afterBucketRemoved(key.getBucketId(), null);

    verify(context, atMost(1)).resubmitCommand(any());
    await().atMost(Duration.ofSeconds(5))
        .during(Duration.ofSeconds(2))
        .untilAsserted(() -> verify(context, atMost(0)).writeToChannel(any()));
    assertThat(eventDistributor.getRegisteredKeys()).isEqualTo(0);
  }

  @Test
  public void testResubmitAndTimeoutDoNotBothExecute() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    List<byte[]> commandArgs = Arrays.asList("KEY".getBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Arrays.asList(command.getKey()), 0);
    AtomicReference<BlockingCommandListener> listenerRef = new AtomicReference<>(listener);
    EventDistributor eventDistributor = new EventDistributor();

    eventDistributor.registerListener(listener);

    new ConcurrentLoopingThreads(10_000,
        i -> listenerRef.get().process(null, command.getKey()),
        i -> listenerRef.get().timeout(eventDistributor))
            .runWithAction(() -> {
              // Verify that resubmitCommand and timeout have not both been called
              try {
                verify(context, atLeastOnce()).resubmitCommand(any());
                verify(context, never()).writeToChannel(any());
              } catch (AssertionError e) {
                verify(context, never()).resubmitCommand(any());
                verify(context, atLeastOnce()).writeToChannel(any());
              }
              reset(context);
              eventDistributor.removeListener(listenerRef.get());
              listenerRef.set(new BlockingCommandListener(context, command,
                  Arrays.asList(command.getKey()), 0));
              eventDistributor.registerListener(listenerRef.get());
            });
  }

  @Test
  public void concurrencyOfManyRegistrationsForTheSameKeys() {
    ExecutionHandlerContext context = mock(ExecutionHandlerContext.class);
    EventDistributor distributor = new EventDistributor();
    RedisKey keyA = new RedisKey("keyA".getBytes());
    RedisKey keyB = new RedisKey("keyB".getBytes());
    RedisKey keyC = new RedisKey("keyC".getBytes());

    // Should not produce any exceptions
    new ConcurrentLoopingThreads(10_000,
        i -> registerListener(distributor, context, keyA, keyB, keyC),
        i -> registerListener(distributor, context, keyB, keyC, keyA),
        i -> registerListener(distributor, context, keyC, keyA, keyB),
        i -> distributor.afterBucketRemoved(keyA.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyB.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyC.getBucketId(), null),
        i -> distributor.fireEvent(null, keyA),
        i -> distributor.fireEvent(null, keyB),
        i -> distributor.fireEvent(null, keyC))
            .run();

    await().atMost(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(distributor.getRegisteredKeys()).isEqualTo(0));
  }

  private void registerListener(EventDistributor distributor, ExecutionHandlerContext context,
      RedisKey... keys) {
    List<byte[]> commandArgs = Arrays.asList("KEY".getBytes(), "0".getBytes());
    Command command = new Command(RedisCommandType.BLPOP, commandArgs);
    BlockingCommandListener listener =
        new BlockingCommandListener(context, command, Arrays.asList(keys), 1e-9);
    distributor.registerListener(listener);
  }
}
