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
import static org.mockito.Mockito.mock;

import org.junit.Test;

import org.apache.geode.redis.internal.netty.Client;

public class ClientSubscriptionManagerImplTest {

  private ClientSubscriptionManager createManager(Client client, Subscription subscription) {
    return new ClientSubscriptionManagerImpl(client, subscription);
  }

  private ClientSubscriptionManager createManager() {
    return createManager(mock(Client.class), mock(Subscription.class));
  }

  @Test
  public void newManagerHasOneSubscription() {
    assertThat(createManager().getSubscriptionCount()).isOne();
  }

  @Test
  public void afterRemoveManagerIsEmpty() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, subscription);

    manager.remove(client);

    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void afterManagerIsEmptyAddFails() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, subscription);

    boolean removeResult = manager.remove(client);
    boolean addResult = manager.add(client, subscription);

    assertThat(removeResult).isFalse();
    assertThat(addResult).isFalse();
    assertThat(manager.getSubscriptionCount()).isZero();
  }

  @Test
  public void secondAddReturnsTrue() {
    byte[] channel = stringToBytes("channel");
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, subscription);
    Client client2 = mock(Client.class);
    Subscription subscription2 = mock(Subscription.class);

    boolean result = manager.add(client2, subscription2);

    assertThat(result).isTrue();
    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
  }

  @Test
  public void removalThatReturnsTrueAllowsMoreAdds() {
    Client client = mock(Client.class);
    Subscription subscription = mock(Subscription.class);
    ClientSubscriptionManager manager = createManager(client, subscription);
    Client client2 = mock(Client.class);
    Subscription subscription2 = mock(Subscription.class);

    manager.add(client2, subscription2);
    boolean removeResult = manager.remove(client);
    boolean addResult = manager.add(client, subscription);

    assertThat(removeResult).isTrue();
    assertThat(addResult).isTrue();
    assertThat(manager.getSubscriptionCount()).isEqualTo(2);
  }
}
