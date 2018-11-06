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
package org.apache.geode.internal.cache.functions;

import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;

public class DistributedRegionFunctionFunctionInvocationException extends FunctionAdapter {
  private int retryCount;

  private boolean isHA;

  private int count;

  public DistributedRegionFunctionFunctionInvocationException(boolean isHA, int retryCount) {
    this.isHA = isHA;
    this.retryCount = retryCount;
    this.count = 0;
  }

  public void execute(FunctionContext context) {
    this.count++;
    if (retryCount != 0 && count >= retryCount) {
      context.getResultSender().lastResult(new Integer(5));
    } else {
      throw new FunctionInvocationTargetException(
          "I have been thrown from DistributedRegionFunctionFunctionInvocationException");
    }
  }

  public String getId() {
    return "DistributedRegionFunctionFunctionInvocationException";
  }

  public boolean isHA() {
    return this.isHA;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }
}
