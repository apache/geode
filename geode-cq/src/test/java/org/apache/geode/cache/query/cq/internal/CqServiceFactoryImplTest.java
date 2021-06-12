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

import static java.util.Collections.singletonMap;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.query.cq.internal.command.CloseCQ;
import org.apache.geode.cache.query.cq.internal.command.ExecuteCQ61;
import org.apache.geode.cache.query.cq.internal.command.GetCQStats;
import org.apache.geode.cache.query.cq.internal.command.GetDurableCQs;
import org.apache.geode.cache.query.cq.internal.command.MonitorCQ;
import org.apache.geode.cache.query.cq.internal.command.StopCQ;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.MessageType;
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

    final CommandRegistry commandRegistry = mock(CommandRegistry.class);

    final CqServiceFactoryImpl cqServiceFactory = new CqServiceFactoryImpl();
    cqServiceFactory.initialize();
    cqServiceFactory.create(internalCache, commandRegistry);

    verify(commandRegistry).register(MessageType.EXECUTECQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, ExecuteCQ61.getCommand()));
    verify(commandRegistry).register(MessageType.EXECUTECQ_WITH_IR_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, ExecuteCQ61.getCommand()));
    verify(commandRegistry).register(MessageType.GETCQSTATS_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, GetCQStats.getCommand()));
    verify(commandRegistry).register(MessageType.MONITORCQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, MonitorCQ.getCommand()));
    verify(commandRegistry).register(MessageType.STOPCQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, StopCQ.getCommand()));
    verify(commandRegistry).register(MessageType.CLOSECQ_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, CloseCQ.getCommand()));
    verify(commandRegistry).register(MessageType.GETDURABLECQS_MSG_TYPE,
        singletonMap(KnownVersion.OLDEST, GetDurableCQs.getCommand()));

    verifyNoMoreInteractions(commandRegistry);
  }

}
