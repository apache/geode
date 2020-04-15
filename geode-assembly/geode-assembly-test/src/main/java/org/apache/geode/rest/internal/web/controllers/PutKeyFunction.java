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

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;


/**
 * Function that puts the value from the argument at the key passed in through the filter.
 */

public class PutKeyFunction implements Function {

  private static final String ID = "PutKeyFunction";

  @Override
  public void execute(FunctionContext context) {
    RegionFunctionContext regionContext = (RegionFunctionContext) context;
    Region dataSet = regionContext.getDataSet();
    Object key = regionContext.getFilter().iterator().next();
    Object value = regionContext.getArguments();
    dataSet.put(key, value);
    context.getResultSender().lastResult(Boolean.TRUE);
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  public void init(Properties p) {}

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }
}
