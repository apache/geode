/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.handlers;

import java.io.IOException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationRequest;
import com.gemstone.gemfire.management.internal.configuration.messages.ConfigurationResponse;

/***
 * Handler for {@link ConfigurationRequest} request message.
 * Processes the {@link ConfigurationRequest}, sends the {@link ConfigurationResponse} containing the requested configuration.
 * @author bansods
 *
 */
public class ConfigurationRequestHandler implements TcpHandler{
  private static final Logger logger = LogService.getLogger();

  SharedConfiguration sharedConfig;

  public ConfigurationRequestHandler(SharedConfiguration sharedConfig) {
    this.sharedConfig = sharedConfig;
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    assert request instanceof ConfigurationRequest;
    try{
      logger.info("Received request for configuration  : {}", request);
      ConfigurationRequest configRequest = (ConfigurationRequest)request;
      return sharedConfig.createConfigurationReponse(configRequest);    
    } catch (Exception e) {
      logger.info(e.getMessage(), e);
      return null;
    }
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
    if (sharedConfig != null) {
      this.sharedConfig = sharedConfig;
    }
  }


}
