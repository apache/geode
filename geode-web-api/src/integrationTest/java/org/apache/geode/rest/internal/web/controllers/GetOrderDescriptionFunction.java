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
package org.apache.geode.rest.internal.web.controllers;

import java.util.Iterator;
import java.util.Set;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;

public class GetOrderDescriptionFunction implements Function<String> {

  @Override
  public void execute(FunctionContext<String> context) {
    InternalCache cache;
    Region<?, ?> region;
    try {
      cache = (InternalCache) CacheFactory.getAnyInstance();
      cache.getCacheConfig().setPdxReadSerialized(true);
      region = cache.getRegion("orders");
    } catch (CacheClosedException ex) {
      context.getResultSender().lastResult("NoCacheFoundResult");
      throw ex;
    }

    RegionFunctionContext regionContext = (RegionFunctionContext) context;
    @SuppressWarnings("unchecked")
    Set<String> keys = (Set<String>) regionContext.getFilter();
    Iterator<String> keysIterator = keys.iterator();
    Object key = null;
    if (keysIterator.hasNext()) {
      key = (keysIterator.next());
    }

    Object obj = region.get(key);
    String description;
    if (obj instanceof PdxInstance) {
      PdxInstance pi = (PdxInstance) obj;
      Order receivedOrder = (Order) pi.getObject();
      description = receivedOrder.getDescription();
    } else {
      Order receivedOrder = (Order) obj;
      description = receivedOrder.getDescription();
    }

    context.getResultSender().lastResult(description);
  }

  @Override
  public String getId() {
    return "GetOrderDescriptionFunction";
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
