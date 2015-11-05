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
package com.gemstone.gemfire.management.internal.configuration.handlers;

import java.io.IOException;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.SharedConfigurationStatusRequest;

public class SharedConfigurationStatusRequestHandler implements TcpHandler {
  
  
  @Override
  public Object processRequest(Object request) throws IOException {
    assert request instanceof SharedConfigurationStatusRequest;
    InternalLocator locator = InternalLocator.getLocator();
    return locator.getSharedConfigurationStatus();
  }

  @Override
  public void endRequest(Object request, long startTime) {

  }

  @Override
  public void endResponse(Object request, long startTime) {

  }

  @Override
  public void shutDown() {

  }

  @Override
  public void init(TcpServer tcpServer) {

  }
  
  @Override
  public void restarting(DistributedSystem system, GemFireCache cache, SharedConfiguration sharedConfig) {

  }

}
