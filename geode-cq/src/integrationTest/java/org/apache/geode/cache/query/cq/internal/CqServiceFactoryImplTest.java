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

package org.apache.geode.cache.query.cq.internal;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.CommandInitializer;
import org.apache.geode.internal.cache.tier.sockets.CommandRegistry;
import org.apache.geode.internal.serialization.KnownVersion;

public class CqServiceFactoryImplTest {
  @Test
  public void registersCommandsOnCreate() {
    final InternalCache internalCache = mock(InternalCache.class);
    final CancelCriterion cancelCriterion = mock(CancelCriterion.class);
    final DistributedSystem distributedSystem = mock(DistributedSystem.class);
    doNothing().when(cancelCriterion).checkCancelInProgress(null);
    when(internalCache.getCancelCriterion()).thenReturn(cancelCriterion);
    when(internalCache.getDistributedSystem()).thenReturn(distributedSystem);

    CommandRegistry commandRegistry = new CommandInitializer();

    final Integer[] messageTypes = {
        MessageType.EXECUTECQ_MSG_TYPE, MessageType.EXECUTECQ_WITH_IR_MSG_TYPE,
        MessageType.GETCQSTATS_MSG_TYPE, MessageType.MONITORCQ_MSG_TYPE,
        MessageType.STOPCQ_MSG_TYPE, MessageType.CLOSECQ_MSG_TYPE,
        MessageType.GETDURABLECQS_MSG_TYPE};

    final Set<Integer> initialKeys = commandRegistry.get(KnownVersion.OLDEST).keySet();
    assertThat(initialKeys).doesNotContain(messageTypes);

    final CqServiceFactoryImpl cqServiceFactory = new CqServiceFactoryImpl();
    cqServiceFactory.initialize();
    cqServiceFactory.create(internalCache, commandRegistry);

    final Set<Integer> expectedKeys = new HashSet<>(initialKeys);
    expectedKeys.addAll(asList(messageTypes));
    assertThat(commandRegistry.get(KnownVersion.OLDEST)).containsOnlyKeys(expectedKeys);
  }
}
