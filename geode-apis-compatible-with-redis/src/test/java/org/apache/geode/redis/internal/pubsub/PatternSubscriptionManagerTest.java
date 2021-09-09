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
package org.apache.geode.redis.internal.pubsub;

import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class PatternSubscriptionManagerTest extends SubscriptionManagerTestBase {

  @Override
  protected AbstractSubscriptionManager<?> createManager() {
    return new PatternSubscriptionManager();
  }

  @Test
  public void managerWithMultiplePatternsAndClients_foreachDoesExpectedIterations() {
    byte[] channel1 = stringToBytes("channel1");
    byte[] pattern1 = stringToBytes("*1");
    byte[] pattern2 = stringToBytes("*2");
    byte[] pattern3 = stringToBytes("*3");

    AtomicInteger count = new AtomicInteger();
    Client client1 = createClient();
    Client client2 = createClient();
    Client client3 = createClient();
    AbstractSubscriptionManager<?> manager = createManager();
    manager.add(pattern1, client1);
    manager.add(pattern1, client2);
    manager.add(pattern1, client3);
    manager.add(pattern2, client2);
    manager.add(pattern3, client3);

    manager.foreachSubscription(channel1, sub -> count.getAndIncrement());

    assertThat(count.get()).isEqualTo(3);
  }

  @Test
  public void twoPatternsThatMatchSameChannel() {
    Client client1 = createClient();
    Client client2 = createClient();
    AbstractSubscriptionManager<?> manager = createManager();
    byte[] channel = stringToBytes("channel");
    byte[] pattern1 = stringToBytes("ch*");
    byte[] pattern2 = stringToBytes("chan*");

    Object result1 = manager.add(pattern1, client1);
    Object result2 = manager.add(pattern2, client2);

    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(result1).isNotNull();
    assertThat(result2).isNotNull();
    assertThat(manager.getSubscriptionCount(channel)).isEqualTo(2);
    assertThat(manager.getIds()).containsExactlyInAnyOrder(pattern1, pattern2);
    assertThat(manager.getIds(stringToBytes("cha*"))).containsExactlyInAnyOrder(pattern2);
  }

}
