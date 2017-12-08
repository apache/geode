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
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * The GetRegions class is an gemfire function that gives data about available regions.
 * <p/>
 *
 * @since GemFire 8.0
 */

public class GetRegions implements Function {

  public void execute(FunctionContext context) {

    ArrayList<String> vals = new ArrayList<String>();

    Cache c = null;
    try {
      c = CacheFactory.getAnyInstance();
    } catch (CacheClosedException ex) {
      vals.add("NoCacheResult");
      context.getResultSender().lastResult(vals);
    }

    final Set<Region<?, ?>> regionSet = c.rootRegions();
    for (Region<?, ?> r : regionSet) {
      vals.add(r.getName());
    }

    context.getResultSender().lastResult(vals);
  }

  public String getId() {
    return "GetRegions";
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
};
