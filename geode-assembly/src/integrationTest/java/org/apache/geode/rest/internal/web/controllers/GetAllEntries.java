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

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;

/**
 * The GetAllEntries is function that will return a map as a result of its execution.
 * <p/>
 *
 * @since GemFire 8.0
 */

public class GetAllEntries implements Function {

  @Override
  public void execute(FunctionContext context) {
    Map<String, String> myMap = new HashMap();
    myMap.put("k11", "v1");
    myMap.put("k12", "v2");
    myMap.put("k13", "v3");
    myMap.put("k14", "v4");
    myMap.put("k15", "v5");

    // return map as a function result
    context.getResultSender().lastResult(myMap);
  }

  @Override
  public String getId() {
    return "GetAllEntries";
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
