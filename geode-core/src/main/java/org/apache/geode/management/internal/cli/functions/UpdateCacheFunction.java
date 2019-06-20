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

package org.apache.geode.management.internal.cli.functions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.GatewayReceiverConfig;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.internal.CacheElementOperation;
import org.apache.geode.management.internal.configuration.realizers.ConfigurationRealizer;
import org.apache.geode.management.internal.configuration.realizers.GatewayReceiverRealizer;
import org.apache.geode.management.internal.configuration.realizers.RegionConfigRealizer;

public class UpdateCacheFunction implements InternalFunction<List> {
  private static final Logger logger = LogService.getLogger();
  @Immutable
  private static final Map<Class, ConfigurationRealizer> realizers = new HashMap<>();

  static {
    realizers.put(RegionConfig.class, new RegionConfigRealizer());
    realizers.put(GatewayReceiverConfig.class, new GatewayReceiverRealizer());
  }

  @Override
  public void execute(FunctionContext<List> context) {
    try {
      context.getResultSender().lastResult(executeFunction(context));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      context.getResultSender().lastResult(new RealizationResult()
          .setSuccess(false)
          .setMemberName(context.getMemberName())
          .setMessage(e.getMessage()));
    }
  }

  public RealizationResult executeFunction(FunctionContext<List> context) {
    CacheElement cacheElement = (CacheElement) context.getArguments().get(0);
    CacheElementOperation operation = (CacheElementOperation) context.getArguments().get(1);
    Cache cache = context.getCache();

    ConfigurationRealizer realizer = realizers.get(cacheElement.getClass());

    RealizationResult result = new RealizationResult();
    result.setMemberName(context.getMemberName());

    if (realizer == null) {
      return result.setMessage(
          "Server needs to be restarted for this configuration change to be realized.");
    }

    switch (operation) {
      case CREATE:
        if (realizer.exists(cacheElement, cache)) {
          return result.setMessage(
              "Element with id=" + cacheElement.getId() + " already exists. Skipp creation.");
        }
        result = realizer.create(cacheElement, cache);
        break;
      case DELETE:
        if (!realizer.exists(cacheElement, cache)) {
          return result.setMessage(
              "Element with id=" + cacheElement.getId() + " does not exist.");
        }
        result = realizer.delete(cacheElement, cache);
        break;
      case UPDATE:
        if (!realizer.exists(cacheElement, cache)) {
          return result.setSuccess(false).setMessage(
              "Element with id=" + cacheElement.getId() + " does not exist.");
        }
        result = realizer.update(cacheElement, cache);
        break;
    }
    result.setMemberName(context.getMemberName());
    return result;
  }
}
