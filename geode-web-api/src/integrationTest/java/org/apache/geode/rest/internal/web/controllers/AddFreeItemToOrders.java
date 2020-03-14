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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxInstance;

/**
 * Gemfire function to add free items in the existing order if the total price for that order is
 * greater then the argument
 */
public class AddFreeItemToOrders implements Function<Object> {

  @Override
  public void execute(FunctionContext<Object> context) {
    List<Object> argsList = new ArrayList<>();
    Object[] argsArray;

    if (context.getArguments() instanceof Object[]) {
      argsArray = (Object[]) context.getArguments();
      argsList = Arrays.asList(argsArray);
    } else if (!((context.getArguments() instanceof Boolean)
        || (context.getArguments() instanceof Vector)
        || (context.getArguments() instanceof String))) {
      System.out.println("AddFreeItemToOrders : Invalid Arguments");
    }

    InternalCache cache;
    Region<Object, Order> region;
    try {
      cache = (InternalCache) CacheFactory.getAnyInstance();
      cache.getCacheConfig().setPdxReadSerialized(true);
      region = cache.getRegion("orders");
    } catch (CacheClosedException ex) {
      List<Object> vals = new ArrayList<>();
      vals.add("NoCacheFoundResult");
      context.getResultSender().lastResult(vals);
      throw ex;
    }

    String oql =
        "SELECT DISTINCT entry.key FROM /orders.entries entry WHERE entry.value.totalPrice > $1";
    Object[] queryArgs = new Object[1];
    queryArgs[0] = argsList.get(0);

    final Query query = cache.getQueryService().newQuery(oql);

    SelectResults<?> result;
    List<Object> keys = new ArrayList<>();
    try {
      result = (SelectResults<?>) query.execute(queryArgs);
      if (result != null) {
        keys.addAll(result);
      }
    } catch (FunctionDomainException e) {
      cache.getLogger()
          .info("Caught FunctionDomainException while executing function AddFreeItemToOrders: "
              + e.getMessage());
    } catch (TypeMismatchException e) {
      cache.getLogger()
          .info("Caught TypeMismatchException while executing function AddFreeItemToOrders: "
              + e.getMessage());
    } catch (NameResolutionException e) {
      cache.getLogger()
          .info("Caught NameResolutionException while executing function AddFreeItemToOrders: "
              + e.getMessage());
    } catch (QueryInvocationTargetException e) {
      cache.getLogger().info(
          "Caught QueryInvocationTargetException while executing function AddFreeItemToOrders"
              + e.getMessage());
    }

    // class has to be in classpath.
    try {
      Item it = (Item) (argsList.get(1));
      for (Object key : keys) {
        Object obj = region.get(key);
        if (obj instanceof PdxInstance) {
          PdxInstance pi = (PdxInstance) obj;
          Order receivedOrder = (Order) pi.getObject();
          receivedOrder.addItem(it);

          region.put(key, receivedOrder);
        }
      }

      context.getResultSender().lastResult("success");
    } catch (Exception e) {
      context.getResultSender().lastResult("failure");
    }
  }

  @Override
  public String getId() {
    return "AddFreeItemToOrders";
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
