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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;

public class EventDistributorTest {

  public static class TestEventListener implements EventListener {
    private final List<RedisKey> keys;
    private int fired = 0;

    public TestEventListener(RedisKey... keys) {
      this.keys = Arrays.stream(keys).collect(Collectors.toList());
    }

    public int getFired() {
      return fired;
    }

    @Override
    public EventResponse process(RedisCommandType commandType, RedisKey key) {
      fired += 1;
      return EventResponse.REMOVE_AND_STOP;
    }

    @Override
    public List<RedisKey> keys() {
      return keys;
    }

    @Override
    public void resubmitCommand() {}

    @Override
    public void scheduleTimeout(ScheduledExecutorService executor, EventDistributor distributor) {}
  }

  @Test
  public void firingEventRemovesListener() {
    RedisKey keyA = new RedisKey("a".getBytes());
    RedisKey keyB = new RedisKey("b".getBytes());
    EventDistributor distributor = new EventDistributor();
    TestEventListener listener = new TestEventListener(keyA, keyB);
    distributor.registerListener(listener);

    distributor.fireEvent(null, keyA);
    assertThat(listener.getFired()).isEqualTo(1);
    assertThat(distributor.getRegisteredKeys()).isEqualTo(0);
  }

  @Test
  public void firingEventRemovesFirstListener_whenMultipleExist() {
    RedisKey keyA = new RedisKey("a".getBytes());
    RedisKey keyB = new RedisKey("b".getBytes());
    EventDistributor distributor = new EventDistributor();
    TestEventListener listener1 = new TestEventListener(keyA, keyB);
    TestEventListener listener2 = new TestEventListener(keyA, keyB);
    distributor.registerListener(listener1);
    distributor.registerListener(listener2);

    assertThat(distributor.getRegisteredKeys()).isEqualTo(4);

    distributor.fireEvent(null, keyA);
    assertThat(listener1.getFired()).isEqualTo(1);
    assertThat(listener2.getFired()).isEqualTo(0);
    assertThat(distributor.getRegisteredKeys()).isEqualTo(2);

    distributor.fireEvent(null, keyA);
    assertThat(listener1.getFired()).isEqualTo(1);
    assertThat(listener2.getFired()).isEqualTo(1);
    assertThat(distributor.getRegisteredKeys()).isEqualTo(0);
  }

  @Test
  public void listenerIsRemovedAfterBucketMoves() {
    RedisKey keyA = new RedisKey("a".getBytes());
    RedisKey keyB = new RedisKey("b".getBytes());
    EventDistributor distributor = new EventDistributor();
    TestEventListener listener1 = new TestEventListener(keyA);
    distributor.registerListener(listener1);
    distributor.registerListener(new TestEventListener(keyB));

    assertThat(distributor.getRegisteredKeys()).isEqualTo(2);

    distributor.afterBucketRemoved(keyA.getBucketId(), null);

    assertThat(distributor.getRegisteredKeys()).isEqualTo(1);
  }

  @Test
  public void concurrencyOfManyRegistrationsAndBucketMovementForTheSameKeys() {
    EventDistributor distributor = new EventDistributor();
    RedisKey keyA = new RedisKey("keyA".getBytes());
    RedisKey keyB = new RedisKey("keyB".getBytes());
    RedisKey keyC = new RedisKey("keyC".getBytes());

    // Should not produce any exceptions
    new ConcurrentLoopingThreads(10_000,
        i -> distributor.registerListener(new TestEventListener(keyA, keyB, keyC)),
        i -> distributor.registerListener(new TestEventListener(keyB, keyC, keyA)),
        i -> distributor.registerListener(new TestEventListener(keyC, keyA, keyB)),
        i -> distributor.afterBucketRemoved(keyA.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyB.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyC.getBucketId(), null),
        i -> distributor.fireEvent(null, keyA),
        i -> distributor.fireEvent(null, keyB),
        i -> distributor.fireEvent(null, keyC))
            .runInLockstep();

    distributor.fireEvent(null, keyA);
    distributor.fireEvent(null, keyB);
    distributor.fireEvent(null, keyC);
    assertThat(distributor.getRegisteredKeys()).isEqualTo(0);
  }

  @Test
  public void concurrencyOfManyRegistrationAndRemovalOfSameListener() {
    EventDistributor distributor = new EventDistributor();
    RedisKey keyA = new RedisKey("keyA".getBytes());
    RedisKey keyB = new RedisKey("keyB".getBytes());
    RedisKey keyC = new RedisKey("keyC".getBytes());
    AtomicReference<EventListener> listenerRef1 =
        new AtomicReference<>(new TestEventListener(keyA, keyB, keyC));
    AtomicReference<EventListener> listenerRef2 =
        new AtomicReference<>(new TestEventListener(keyB, keyC, keyA));
    AtomicReference<EventListener> listenerRef3 =
        new AtomicReference<>(new TestEventListener(keyC, keyA, keyB));

    // Should not produce any exceptions
    new ConcurrentLoopingThreads(10_000,
        i -> distributor.registerListener(listenerRef1.get()),
        i -> distributor.registerListener(listenerRef2.get()),
        i -> distributor.registerListener(listenerRef3.get()),
        i -> distributor.removeListener(listenerRef1.get()),
        i -> distributor.removeListener(listenerRef2.get()),
        i -> distributor.removeListener(listenerRef3.get()),
        i -> distributor.fireEvent(null, keyA),
        i -> distributor.fireEvent(null, keyB),
        i -> distributor.fireEvent(null, keyC))
            .runWithAction(() -> {
              listenerRef1.set(new TestEventListener(keyA, keyB, keyC));
              listenerRef2.set(new TestEventListener(keyB, keyC, keyA));
              listenerRef3.set(new TestEventListener(keyC, keyA, keyB));
            });

    distributor.fireEvent(null, keyA);
    distributor.fireEvent(null, keyB);
    distributor.fireEvent(null, keyC);
    assertThat(distributor.getRegisteredKeys()).isEqualTo(0);
  }

  /**
   * When a key points to a Queue (of listeners) that becomes empty that key is
   * removed. Without correct synchronization we could add a new listener to a queue
   * just before the key (and thus the queue) is removed. This test ensures that is
   * not happening.
   */
  @Test
  public void ensureNotRegisteringListenerOnQueueJustRemoved() {
    EventDistributor distributor = new EventDistributor();
    RedisKey keyA = new RedisKey("keyA".getBytes());
    AtomicReference<EventListener> listenerRef1 =
        new AtomicReference<>(new TestEventListener(keyA));
    AtomicReference<EventListener> listenerRef2 =
        new AtomicReference<>(new TestEventListener(keyA));

    distributor.registerListener(listenerRef1.get());
    AtomicInteger iteration = new AtomicInteger(0);

    new ConcurrentLoopingThreads(10_000,
        iteration::set,
        i -> distributor.registerListener(listenerRef2.get()),
        i -> distributor.removeListener(listenerRef1.get()))
            .runWithAction(() -> {
              assertThat(distributor.getRegisteredKeys())
                  .as("Iteration = " + iteration.get()).isEqualTo(1);
              distributor.removeListener(listenerRef2.get());

              listenerRef1.set(new TestEventListener(keyA));
              listenerRef2.set(new TestEventListener(keyA));

              distributor.registerListener(listenerRef1.get());
            });
  }

}
