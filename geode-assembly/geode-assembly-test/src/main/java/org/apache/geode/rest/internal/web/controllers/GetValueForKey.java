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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

public class GetValueForKey implements Function {

  @Override
  public void execute(FunctionContext context) {
    Object args = context.getArguments();

    Cache cache = null;

    try {
      cache = CacheFactory.getAnyInstance();

      if (args.toString().equalsIgnoreCase("1")) {
        Region<String, Object> r = cache.getRegion("Products");
        Object result = r.get("1");
        context.getResultSender().lastResult(result);

      } else if (args.toString().equalsIgnoreCase("2")) {
        Region<String, Object> r = cache.getRegion("People");
        Object result = r.get("2");
        context.getResultSender().lastResult(result);
      } else {
        // Default case
        int i = 10;
        context.getResultSender().lastResult(i);
      }
    } catch (CacheClosedException e) {
      context.getResultSender().lastResult("Error: CacheClosedException");
    }

  }

  @Override
  public String getId() {
    return "GetValueForKey";
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
