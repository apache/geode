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
package org.apache.geode.internal.cache.wan.serial;

import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * @since GemFire 7.0
 *
 */
public class SerialSecondaryGatewayListener extends CacheListenerAdapter {

  private static final Logger logger = LogService.getLogger();

  private final SerialGatewaySenderEventProcessor processor;

  private final AbstractGatewaySender sender;

  protected SerialSecondaryGatewayListener(SerialGatewaySenderEventProcessor eventProcessor) {
    processor = eventProcessor;
    sender = eventProcessor.getSender();
  }

  @Override
  public void afterCreate(EntryEvent event) {
    if (sender.isPrimary()) {
      // The secondary has failed over to become the primary. There is a small
      // window where the secondary has become the primary, but the listener
      // is
      // still set. Ignore any updates to the map at this point. It is unknown
      // what the state of the map is. This may result in duplicate events
      // being sent.
      return;
    }
    // There is a small window where queue has not been created fully yet.
    // The underlying region of the queue is created, and it receives afterDestroy callback
    final Set<RegionQueue> queues = sender.getQueues();
    if (queues != null && !queues.isEmpty()) {
      sender.getStatistics().incQueueSize();
    }
    // fix bug 35730

    // Send event to the event dispatcher
    GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl) event.getNewValue();
    processor.handlePrimaryEvent(senderEvent);
  }

  @Override
  public void afterDestroy(EntryEvent event) {
    if (sender.isPrimary()) {
      return;
    }
    // fix bug 37603
    // There is a small window where queue has not been created fully yet. The region is created,
    // and it receives afterDestroy callback.

    final Set<RegionQueue> queues = sender.getQueues();
    if (queues != null && !queues.isEmpty()) {
      sender.getStatistics().decQueueSize();
    }

    // Send event to the event dispatcher
    Object oldValue = event.getOldValue();
    if (oldValue instanceof GatewaySenderEventImpl) {
      GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl) oldValue;
      if (logger.isDebugEnabled()) {
        logger.debug("Received after Destroy for Secondary event {} the key was {}", senderEvent,
            event.getKey());
      }
      processor.handlePrimaryDestroy(senderEvent);
    }
  }
}
