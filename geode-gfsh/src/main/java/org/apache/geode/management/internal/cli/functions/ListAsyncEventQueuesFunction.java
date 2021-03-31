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
package org.apache.geode.management.internal.cli.functions;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.internal.cli.domain.AsyncEventQueueDetails;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * An implementation of GemFire Function interface used to determine all the async event queues that
 * exist for the entire cache, distributed across the GemFire distributed system.
 * </p>
 *
 * @since GemFire 8.0
 */
public class ListAsyncEventQueuesFunction extends CliFunction {
  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.ListAsyncEventQueuesFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public CliFunctionResult executeFunction(final FunctionContext context) {
    Cache cache = context.getCache();
    DistributedMember member = cache.getDistributedSystem().getDistributedMember();

    // Identify by name if the name is non-trivial. Otherwise, use the ID
    String memberId = !member.getName().equals("") ? member.getName() : member.getId();

    Set<AsyncEventQueue> asyncEventQueues = cache.getAsyncEventQueues();

    List<AsyncEventQueueDetails> details = asyncEventQueues.stream().map(queue -> {
      AsyncEventListener listener = queue.getAsyncEventListener();
      Properties listenerProperties = new Properties();
      if (listener instanceof Declarable2) {
        listenerProperties = ((Declarable2) listener).getConfig();
      }

      return new AsyncEventQueueDetails(queue.getId(), queue.getBatchSize(), queue.isPersistent(),
          queue.getDiskStoreName(), queue.getMaximumQueueMemory(), listener.getClass().getName(),
          listenerProperties, isCreatedWithPausedEventDispatching(queue),
          queue.isDispatchingPaused());
    }).collect(Collectors.toList());

    return new CliFunctionResult(memberId, details);
  }

  private boolean isCreatedWithPausedEventDispatching(AsyncEventQueue queue) {
    return ((AbstractGatewaySender) ((AsyncEventQueueImpl) queue).getSender())
        .isStartEventProcessorInPausedState();
  }
}
