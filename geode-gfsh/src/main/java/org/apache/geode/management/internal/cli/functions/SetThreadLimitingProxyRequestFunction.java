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


import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.ProxyRequestObserver;
import org.apache.geode.cache.ProxyRequestObserverHolder;
import org.apache.geode.cache.ThreadLimitingProxyRequestObserver;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Class for change log level function
 *
 * @since 8.0
 */
public class SetThreadLimitingProxyRequestFunction implements InternalFunction<Object[]> {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = 1L;

  private static final String ID =
      "org.apache.geode.management.internal.cli.functions.SetThreadLimitingProxyRequestFunction";

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void execute(FunctionContext<Object[]> context) {
    InternalCache cache = (InternalCache) context.getCache();
    Map<String, String> result = new HashMap<>();
    try {
      Object[] args = context.getArguments();
      Integer maxThreads = (Integer) args[0];

      ProxyRequestObserver observer = null;

      if (maxThreads > 0) {
        observer = new ThreadLimitingProxyRequestObserver(maxThreads);
      }

      ProxyRequestObserverHolder.setInstance(observer);

      result.put(cache.getDistributedSystem().getDistributedMember().getId(),
          "Set thread liminting proxy request to: " + maxThreads);
      context.getResultSender().lastResult(result);
    } catch (Exception ex) {
      logger.info(LogMarker.CONFIG_MARKER, "GFSH exception {}", ex.getMessage(),
          ex);
      result.put(cache.getDistributedSystem().getDistributedMember().getId(),
          "Exception " + ex.getMessage());
      context.getResultSender().lastResult(result);
    }
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
