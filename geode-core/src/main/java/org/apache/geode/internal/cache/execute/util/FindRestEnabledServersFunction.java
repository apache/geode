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
package org.apache.geode.internal.cache.execute.util;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.management.internal.RestAgent;

/**
 * The FindRestEnabledServersFunction class is a gemfire function that gives details about REST
 * enabled gemfire servers.
 *
 * @since GemFire 8.1
 */
public class FindRestEnabledServersFunction implements InternalFunction {
  private static final long serialVersionUID = 7851518767859544678L;

  /**
   * This property defines internal function that will get executed on each node to fetch active
   * REST service endpoints (servers).
   */
  public static final String FIND_REST_ENABLED_SERVERS_FUNCTION_ID =
      FindRestEnabledServersFunction.class.getName();

  @Override
  public void execute(FunctionContext context) {
    try {
      InternalCache cache = (InternalCache) context.getCache();
      DistributionConfig config = cache.getInternalDistributedSystem().getConfig();

      String bindAddress = RestAgent.getBindAddressForHttpService(config);

      final String protocolType = config.getHttpServiceSSLEnabled() ? "https" : "http";

      if (cache.isRESTServiceRunning()) {
        context.getResultSender()
            .lastResult(protocolType + "://" + bindAddress + ":" + config.getHttpServicePort());

      } else {
        context.getResultSender().lastResult("");
      }
    } catch (CacheClosedException ex) {
      context.getResultSender().lastResult("");
    }
  }

  @Override
  public String getId() {
    return FIND_REST_ENABLED_SERVERS_FUNCTION_ID;
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
