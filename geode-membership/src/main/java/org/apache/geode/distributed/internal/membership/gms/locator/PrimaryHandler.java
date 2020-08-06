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
package org.apache.geode.distributed.internal.membership.gms.locator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PrimaryHandler implements TcpHandler {
  private static final Logger logger = LogService.getLogger();

  private final TcpHandler fallbackHandler;

  private volatile Map<Class<?>, TcpHandler> handlerMapping = new HashMap<>();
  private volatile Set<TcpHandler> allHandlers = new HashSet<>();

  private TcpServer tcpServer;
  private int locatorWaitTime;

  @FunctionalInterface
  interface Sleeper {
    void sleep(long msToSleep) throws InterruptedException;
  }

  @FunctionalInterface
  interface MillisecondProvider {
    long millisecondTime();
  }

  private final MillisecondProvider millisecondProvider;
  private Sleeper sleeper;

  PrimaryHandler(TcpHandler fallbackHandler, int locatorWaitTime,
      MillisecondProvider millisecondProvider,
      Sleeper sleeper) {
    this.locatorWaitTime = locatorWaitTime;
    this.fallbackHandler = fallbackHandler;
    this.millisecondProvider = millisecondProvider;
    this.sleeper = sleeper;
    allHandlers.add(fallbackHandler);
  }

  // this method is synchronized to make sure that no new handlers are added while
  // initialization is taking place.
  @Override
  public synchronized void init(TcpServer tcpServer) {
    this.tcpServer = tcpServer;
    for (TcpHandler handler : allHandlers) {
      handler.init(tcpServer);
    }
  }

  @Override
  public Object processRequest(Object request) throws IOException {
    long giveup = 0;
    while (giveup == 0 || millisecondProvider.millisecondTime() < giveup) {
      TcpHandler handler;
      if (request instanceof PeerLocatorRequest) {
        handler = handlerMapping.get(PeerLocatorRequest.class);
      } else {
        handler = handlerMapping.get(request.getClass());
      }

      if (handler != null) {
        return handler.processRequest(request);
      }

      if (fallbackHandler != null) {
        return fallbackHandler.processRequest(request);
      }

      // either there is a configuration problem or the locator is still starting up
      if (giveup == 0) {
        int locatorWaitTime = this.locatorWaitTime;
        if (locatorWaitTime <= 0) {
          // always retry some number of times
          locatorWaitTime = 30;
        }
        giveup = millisecondProvider.millisecondTime() + locatorWaitTime * 1000L;
      }

      try {
        sleeper.sleep(1000);
      } catch (InterruptedException ignored) {
        // running in an executor - no need to set the interrupted flag on the thread
        return null;
      }
    }
    logger.info(
        "Received a location request of class {} but the handler for this is either not enabled or is not ready to process requests",
        request.getClass().getSimpleName());
    return null;
  }

  @Override
  public void shutDown() {
    for (TcpHandler handler : allHandlers) {
      try {
        handler.shutDown();
      } catch (Throwable e) {
        logger.error("Caught exception shutting down handler", e);
      }
    }
  }

  synchronized boolean isHandled(Class<?> clazz) {
    return handlerMapping.containsKey(clazz);
  }

  public synchronized void addHandler(Class<?> clazz, TcpHandler handler) {
    Map<Class<?>, TcpHandler> tmpHandlerMapping = new HashMap<>(handlerMapping);
    Set<TcpHandler> tmpAllHandlers = new HashSet<>(allHandlers);
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
