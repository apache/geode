/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.distributed.internal;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.locator.PeerLocatorRequest;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PrimaryHandler implements RestartableTcpHandler {
  private static final Logger logger = LogService.getLogger();

  private final LocatorMembershipListener locatorListener;
  private final InternalLocator internalLocator;

  private volatile Map<Class, RestartableTcpHandler> handlerMapping = new HashMap<>();
  private volatile Set<RestartableTcpHandler> allHandlers = new HashSet<>();

  private TcpServer tcpServer;

  PrimaryHandler(InternalLocator locator, LocatorMembershipListener listener) {
    locatorListener = listener;
    internalLocator = locator;
  }

  // this method is synchronized to make sure that no new handlers are added while
  // initialization is taking place.
  @Override
  public synchronized void init(TcpServer tcpServer) {
    if (locatorListener != null) {
      // This is deferred until now as the initial requested port could have been 0
      locatorListener.setPort(internalLocator.getPort());
    }
    this.tcpServer = tcpServer;
    for (TcpHandler handler : allHandlers) {
      handler.init(tcpServer);
    }
  }

  @Override
  public void restarting(DistributedSystem ds, GemFireCache cache,
      InternalConfigurationPersistenceService sharedConfig) {
    if (ds != null) {
      for (RestartableTcpHandler handler : allHandlers) {
        handler.restarting(ds, cache, sharedConfig);
      }
    }
  }

  @Override
  public void restartCompleted(DistributedSystem ds) {
    if (ds != null) {
      for (RestartableTcpHandler handler : allHandlers) {
        handler.restartCompleted(ds);
      }
    }
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    long giveup = 0;
    while (giveup == 0 || System.currentTimeMillis() < giveup) {
      TcpHandler handler;
      if (request instanceof PeerLocatorRequest) {
        handler = handlerMapping.get(PeerLocatorRequest.class);
      } else {
        handler = handlerMapping.get(request.getClass());
      }

      if (handler != null) {
        return handler.processRequest(request);
      }

      if (locatorListener != null) {
        return locatorListener.handleRequest(request);
      }

      // either there is a configuration problem or the locator is still starting up
      if (giveup == 0) {
        int locatorWaitTime = internalLocator.getConfig().getLocatorWaitTime();
        if (locatorWaitTime <= 0) {
          // always retry some number of times
          locatorWaitTime = 30;
        }
        giveup = System.currentTimeMillis() + locatorWaitTime * 1000L;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignored) {
          // running in an executor - no need to set the interrupted flag on the thread
          return null;
        }
      }
    }
    logger.info(
        "Received a location request of class {} but the handler for this is either not enabled or is not ready to process requests",
        request.getClass().getSimpleName());
    return null;
  }

  @Override
  public void shutDown() {
    try {
      for (TcpHandler handler : allHandlers) {
        handler.shutDown();
      }
    } finally {
      internalLocator.handleShutdown();
    }
  }

  synchronized boolean isHandled(Class clazz) {
    return handlerMapping.containsKey(clazz);
  }

  public synchronized void addHandler(Class clazz, RestartableTcpHandler handler) {
    Map<Class, RestartableTcpHandler> tmpHandlerMapping = new HashMap<>(handlerMapping);
    Set<RestartableTcpHandler> tmpAllHandlers = new HashSet<>(allHandlers);
    tmpHandlerMapping.put(clazz, handler);
    if (tmpAllHandlers.add(handler) && tcpServer != null) {
      handler.init(tcpServer);
    }
    handlerMapping = tmpHandlerMapping;
    allHandlers = tmpAllHandlers;
  }

  @Override
  public void endRequest(Object request, long startTime) {
    TcpHandler handler = handlerMapping.get(request.getClass());
    if (handler != null) {
      handler.endRequest(request, startTime);
    }
  }

  @Override
  public void endResponse(Object request, long startTime) {
    TcpHandler handler = handlerMapping.get(request.getClass());
    if (handler != null) {
      handler.endResponse(request, startTime);
    }
  }
}
