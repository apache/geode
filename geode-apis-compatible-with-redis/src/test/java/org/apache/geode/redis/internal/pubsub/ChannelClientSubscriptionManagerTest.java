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

import static java.util.Arrays.asList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class ChannelClientSubscriptionManagerTest {

  protected ClientSubscriptionManager createManager(Client client, byte[] pattern,
      Subscription subscription) {
    return new ChannelClientSubscriptionManager(client, subscription);
  }

  protected ClientSubscriptionManager createManager() {
    return createManager(mock(Client.class), stringToBytes("*"), mock(Subscription.class));
  }

  @Test
  public void newManagerHasOneSubscription() {
    assertThat(createManager().getSubscriptionCount()).isOne();
    assertThat(createManager().getSubscriptionCount("channel")).isOne();
  }

  @Test
  public void afterRemoveManagerIsEmpty() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, stringToBytes("*"), subscription);

    manager.remove(client);

    assertThat(manager.getSubscriptionCount()).isZero();
    assertThat(manager.getSubscriptionCount("channel")).isZero();
    Collection<Collection<Object>> foreachResults = new ArrayList<>();
    manager.forEachSubscription(null, "channel",
        (subName, toMatch, c, sub) -> foreachResults.add(asList(subName, toMatch, c, sub)));
    assertThat(foreachResults).isEmpty();
  }

  @Test
  public void afterManagerIsEmptyAddFails() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, stringToBytes("*"), subscription);

    boolean removeResult = manager.remove(client);
    boolean addResult = manager.add(client, subscription);

    assertThat(removeResult).isFalse();
    assertThat(addResult).isFalse();
    assertThat(manager.getSubscriptionCount()).isZero();
    assertThat(manager.getSubscriptionCount("channel")).isZero();
  }

  @Test
  public void secondAddReturnsTrue() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, stringToBytes("*"), subscription);
    Client client2 = mock(Client.class);
    Subscription subscription2 = mock(Subscription.class);

    boolean result = manager.add(client2, subscription2);

    assertThat(result).isTrue();
    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(manager.getSubscriptionCount("channel")).isEqualTo(2);
    Collection<Collection<Object>> foreachResults = new ArrayList<>();
    byte[] subName = stringToBytes("subName");
    manager.forEachSubscription(subName, "channel",
        (sn, toMatch, c, sub) -> foreachResults.add(asList(sn, toMatch, c, sub)));
    assertThat(foreachResults).containsExactlyInAnyOrder(
        asList(subName, "channel", client, subscription),
        asList(subName, "channel", client2, subscription2));
  }

  @Test
  public void removalThatReturnsTrueAllowsMoreAdds() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, stringToBytes("*"), subscription);
    Client client2 = mock(Client.class);
    Subscription subscription2 = mock(Subscription.class);

    manager.add(client2, subscription2);
    boolean removeResult = manager.remove(client);
    boolean addResult = manager.add(client, subscription);


    assertThat(removeResult).isTrue();
    assertThat(addResult).isTrue();
    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
    assertThat(manager.getSubscriptionCount("channel")).isEqualTo(2);
    Collection<Collection<Object>> foreachResults = new ArrayList<>();
    byte[] subName = stringToBytes("subName");
    manager.forEachSubscription(subName, "channel",
        (sn, toMatch, c, sub) -> foreachResults.add(asList(sn, toMatch, c, sub)));
    assertThat(foreachResults).containsExactlyInAnyOrder(
        asList(subName, "channel", client, subscription),
        asList(subName, "channel", client2, subscription2));
  }
}
