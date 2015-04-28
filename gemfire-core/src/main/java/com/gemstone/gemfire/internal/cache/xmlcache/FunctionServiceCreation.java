/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionService;

/**
 * FunctionServiceCreation to be used in CacheXmlParser
 * @author skumar
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
