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
package org.apache.geode.redis.internal.pubsub;

import static java.util.Arrays.asList;
import static org.apache.geode.redis.internal.netty.Coder.stringToBytes;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class PubSubImplTest {
  private final Subscriptions subscriptions = mock(Subscriptions.class);
  private final Publisher publisher = mock(Publisher.class);
  private final PubSubImpl pubsub = new PubSubImpl(subscriptions, publisher);

  @Test
  public void findNumberOfSubscribersPerChannel_handlesEmptyList() {
    List<Object> result = pubsub.findNumberOfSubscribersPerChannel(Collections.emptyList());
    assertThat(result).isEmpty();
  }

  @Test
  public void findNumberOfSubscribersPerChannel_hasCorrectResults() {
    byte[] channel1 = stringToBytes("channel1");
    byte[] channel2 = stringToBytes("channel2");
    when(subscriptions.getChannelSubscriptionCount(channel1)).thenReturn(1);
    when(subscriptions.getChannelSubscriptionCount(channel2)).thenReturn(2);

    List<Object> result = pubsub.findNumberOfSubscribersPerChannel(asList(channel1, channel2));

    assertThat(result).containsExactly(channel1, 1, channel2, 2);
  }

}
