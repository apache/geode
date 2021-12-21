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
package org.apache.geode.internal.cache.execute;

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;


/**
 * This class is intended to be used by ResultCollector classes in order to make sure
 * that the result returned by the getResult method is only
 * computed once. Subsequent invocations to these methods return the result computed
 * in the first invocation.
 *
 */
public class ResultCollectorHolder {

  private volatile boolean resultCollected = false;

  private FunctionException exceptionThrown = null;

  private Object objectReturned = null;

  private final CachedResultCollector rc;

  public ResultCollectorHolder(CachedResultCollector rc) {
    this.rc = rc;
  }

  public Object getResult()
      throws FunctionException {
    if (resultCollected) {
      if (exceptionThrown != null) {
        throw exceptionThrown;
      }
      return objectReturned;
    }
    resultCollected = true;
    try {
      objectReturned = rc.getResultInternal();
      return objectReturned;
    } catch (FunctionException e) {
      exceptionThrown = e;
      throw e;
    }
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    if (resultCollected) {
      if (exceptionThrown != null) {
        throw exceptionThrown;
      }
      return objectReturned;
    }
    resultCollected = true;
    try {
      objectReturned = rc.getResultInternal(timeout, unit);
      return objectReturned;
    } catch (FunctionException e) {
      exceptionThrown = e;
      throw e;
    }
  }

}
