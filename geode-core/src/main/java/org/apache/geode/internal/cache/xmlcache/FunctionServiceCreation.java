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
package org.apache.geode.internal.cache.xmlcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;

/**
 * FunctionServiceCreation to be used in CacheXmlParser
 *
 */
public class FunctionServiceCreation {

  private final List<Function> functions = new ArrayList<>();

  public FunctionServiceCreation() {}

  public void registerFunction(Function f) {
    functions.add(f);
  }

  public void create() {
    for (Function function : functions) {
      FunctionService.registerFunction(function);
    }
  }

  List<Function> getFunctionList() {
    return functions;
  }

  public Map<String, Function> getFunctions() {
    Map<String, Function> result = new HashMap<>();
    for (Function function : functions) {
      result.put(function.getId(), function);
    }
    return result;
  }
}
