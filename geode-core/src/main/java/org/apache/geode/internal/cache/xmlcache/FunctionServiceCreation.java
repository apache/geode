/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;

/**
 * FunctionServiceCreation to be used in CacheXmlParser
 *
 */
public class FunctionServiceCreation {

  private final Map<String, Function> functions = new ConcurrentHashMap<String, Function>();

  public FunctionServiceCreation() {
  }

  public void registerFunction(Function f) {
    this.functions.put(f.getId(), f);
    // Register to FunctionService also so that if somebody does not call
    // FunctionService.create()
    FunctionService.registerFunction(f);     
  }

  public void create() {
    for (Function function : this.functions.values()) {
      FunctionService.registerFunction(function);
    }
  }

  public Map<String, Function> getFunctions() {
    return this.functions;
  }
}
