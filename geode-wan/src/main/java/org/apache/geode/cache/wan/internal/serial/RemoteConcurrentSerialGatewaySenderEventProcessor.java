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
package org.apache.geode.cache.wan.internal.serial;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.serial.ConcurrentSerialGatewaySenderEventProcessor;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class RemoteConcurrentSerialGatewaySenderEventProcessor
    extends ConcurrentSerialGatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();

  public RemoteConcurrentSerialGatewaySenderEventProcessor(AbstractGatewaySender sender,
      ThreadsMonitoring tMonitoring, boolean cleanQueues) {
    super(sender, tMonitoring, cleanQueues);
  }

  @Override
  protected void initializeMessageQueue(String id, boolean cleanQueues) {
    for (int i = 0; i < sender.getDispatcherThreads(); i++) {
      processors.add(new RemoteSerialGatewaySenderEventProcessor(this.sender, id + "." + i,
          getThreadMonitorObj(), cleanQueues));
      if (logger.isDebugEnabled()) {
        logger.debug("Created the RemoteSerialGatewayEventProcessor_{}->{}", i, processors.get(i));
      }
    }
  }
}
