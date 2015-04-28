/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
