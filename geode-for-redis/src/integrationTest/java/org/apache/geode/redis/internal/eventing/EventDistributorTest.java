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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.geode.redis.ConcurrentLoopingThreads;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class EventDistributorTest {

  public static class TestEventListener implements EventListener {
    private final List<RedisKey> keys;
    private int fired = 0;
    private final long timeout;

    public TestEventListener(RedisKey... keys) {
      this(0, keys);
    }

    public TestEventListener(long timeout, RedisKey... keys) {
      this.keys = Arrays.stream(keys).collect(Collectors.toList());
      this.timeout = timeout;
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
    public long getTimeout() {
      return timeout;
    }

    @Override
    public void timeout() {}

    @Override
    public void setCleanupTask(Runnable r) {}

    @Override
    public void cleanup() {}
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
    assertThat(distributor.size()).isEqualTo(0);
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

    assertThat(distributor.size()).isEqualTo(4);

    distributor.fireEvent(null, keyA);
    assertThat(listener1.getFired()).isEqualTo(1);
    assertThat(listener2.getFired()).isEqualTo(0);
    assertThat(distributor.size()).isEqualTo(2);

    distributor.fireEvent(null, keyA);
    assertThat(listener1.getFired()).isEqualTo(1);
    assertThat(listener2.getFired()).isEqualTo(1);
    assertThat(distributor.size()).isEqualTo(0);
  }

  @Test
  public void listenerIsRemovedAfterTimeout() {
    RedisKey keyA = new RedisKey("a".getBytes());
    EventDistributor distributor = new EventDistributor();
    TestEventListener listener1 = new TestEventListener(1, keyA);
    distributor.registerListener(listener1);

    GeodeAwaitility.await().atMost(Duration.ofSeconds(1)).until(() -> distributor.size() == 0);
  }

  @Test
  public void listenerIsRemovedAfterBucketMoves() {
    RedisKey keyA = new RedisKey("a".getBytes());
    RedisKey keyB = new RedisKey("b".getBytes());
    EventDistributor distributor = new EventDistributor();
    TestEventListener listener1 = new TestEventListener(keyA);
    distributor.registerListener(listener1);
    distributor.registerListener(new TestEventListener(keyB));

    assertThat(distributor.size()).isEqualTo(2);

    distributor.afterBucketRemoved(keyA.getBucketId(), null);

    assertThat(distributor.size()).isEqualTo(1);
  }

  @Test
  public void ensureCorrectAccounting_whenManyListenersTimeoutAfterFiring() {
    EventDistributor distributor = new EventDistributor();
    List<RedisKey> keys = new ArrayList<>();
    Random random = new Random();

    for (int i = 0; i < 10_000; i++) {
      RedisKey key = new RedisKey(("key-" + i).getBytes());
      keys.add(key);
      distributor.registerListener(new TestEventListener(100, key));
    }

    for (int i = 0; i < 10_000; i++) {
      distributor.fireEvent(null, keys.get(random.nextInt(10_000)));
    }

    GeodeAwaitility.await().atMost(Duration.ofSeconds(5))
        .during(Duration.ofSeconds(1))
        .until(() -> distributor.size() == 0);
  }

  @Test
  public void concurrencyOfManyRegistrationsForTheSameKeys() {
    EventDistributor distributor = new EventDistributor();
    RedisKey keyA = new RedisKey("keyA".getBytes());
    RedisKey keyB = new RedisKey("keyB".getBytes());
    RedisKey keyC = new RedisKey("keyC".getBytes());

    // Should not produce any exceptions
    new ConcurrentLoopingThreads(10_000,
        i -> distributor.registerListener(new TestEventListener(100, keyA, keyB, keyC)),
        i -> distributor.registerListener(new TestEventListener(100, keyB, keyC, keyA)),
        i -> distributor.registerListener(new TestEventListener(100, keyC, keyA, keyB)),
        i -> distributor.afterBucketRemoved(keyA.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyB.getBucketId(), null),
        i -> distributor.afterBucketRemoved(keyC.getBucketId(), null),
        i -> distributor.fireEvent(null, keyA),
        i -> distributor.fireEvent(null, keyB),
        i -> distributor.fireEvent(null, keyC))
            .run();

    GeodeAwaitility.await().atMost(Duration.ofSeconds(30))
        .during(Duration.ofSeconds(1))
        .untilAsserted(() -> assertThat(distributor.size()).isEqualTo(0));
  }

  @Test
  public void wat() {

  }
}
