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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

/**
 * Default implementation of ResultCollector interface. DefaultResultCollector gathers result from
 * all the function execution nodes<br>
 * Using a custom ResultCollector a user can sort/aggregate the result. This implementation stores
 * the result in a List. The size of the list will be same as the no of nodes on which a function
 * got executed
 *
 * @since GemFire 6.0
 *
 */
public class DefaultResultCollector implements ResultCollector {

  private final ArrayList<Object> resultList = new ArrayList<>();

  public DefaultResultCollector() {}

  /**
   * Adds a single function execution result from a remote node to the ResultCollector
   *
   */
  @Override
  public synchronized void addResult(DistributedMember distributedMember,
      Object resultOfSingleExecution) {
    resultList.add(resultOfSingleExecution);
  }

  /**
   * Returns the result of the execution if available.<br>
   * If {@link Function#hasResult()} is false, upon calling {@link ResultCollector#getResult()}
   * throws {@link FunctionException}.
   *
   * @return the Object computed result
   * @throws FunctionException if something goes wrong while retrieving the result
   */
  @Override
  public Object getResult() throws FunctionException {
    return resultList; // this is full result
  }

  /**
   * Call back provided to caller, which is called after function execution is complete and caller
   * can retrieve results using {@link ResultCollector#getResult()}
   *
   */
  @Override
  public void endResults() {}

  /**
   * Returns the result of the execution if available.<br>
   * If {@link Function#hasResult()} is false, upon calling {@link ResultCollector#getResult()}
   * throws {@link FunctionException}.
   *
   * @param timeout the maximum time to wait (ignored)
   * @param unit the time unit of the timeout argument
   * @return Object computed result
   * @throws FunctionException if something goes wrong while retrieving the result
   */
  @Override
  public Object getResult(long timeout, TimeUnit unit) throws FunctionException {
    return resultList;
  }

  /**
   * GemFire will invoke this method before re-executing function (in case of Function Execution HA)
   * This is to clear the previous execution results from the result collector
   *
   */
  @Override
  public void clearResults() {
    resultList.clear();
  }
}
