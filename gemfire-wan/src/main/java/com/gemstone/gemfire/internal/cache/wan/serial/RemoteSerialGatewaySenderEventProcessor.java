/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan.serial;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackDispatcher;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.logging.LogService;

public class RemoteSerialGatewaySenderEventProcessor extends
    SerialGatewaySenderEventProcessor {

  private static final Logger logger = LogService.getLogger();
  public RemoteSerialGatewaySenderEventProcessor(AbstractGatewaySender sender,
      String id) {
    super(sender, id);
  }

  public void initializeEventDispatcher() {
    if (logger.isDebugEnabled()) {
      logger.debug(" Creating the GatewayEventRemoteDispatcher");
    }
    // In case of serial there is a way to create gatewaysender and attach
    // asynceventlistener. Not sure of the use-case but there are dunit tests
    // To make them passuncommenting the below condition
    if (this.sender.getRemoteDSId() != GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID) {
      this.dispatcher = new GatewaySenderEventRemoteDispatcher(this);
    }else{
      this.dispatcher = new GatewaySenderEventCallbackDispatcher(this);
    }
  }
  
}
