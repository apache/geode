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

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

/**
 * A Special ResultCollector implementation. Functions having {@link Function#hasResult()} false,
 * this ResultCollector will be returned. <br>
 * Calling getResult on this NoResult will throw {@link FunctionException}
 *
 *
 *
 * @since GemFire 5.8 Beta
 *
 * @see Function#hasResult()
 *
 */
public class NoResult implements ResultCollector, Serializable {

  private static final long serialVersionUID = -4901369422864228848L;

  public void addResult(DistributedMember memberID, Object resultOfSingleExecution) {
    throw new UnsupportedOperationException(
        "Cannot add result as the Function#hasResult() is false");
  }

  public void endResults() {
    throw new UnsupportedOperationException(
        "Cannot close result as the Function#hasResult() is false");
  }

  public Object getResult() throws FunctionException {
    throw new FunctionException("Cannot return any result as the Function#hasResult() is false");
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    throw new FunctionException("Cannot return any result as the Function#hasResult() is false");
  }

  public void clearResults() {
    throw new UnsupportedOperationException(
        "Cannot clear result as the Function#hasResult() is false");
  }
}
