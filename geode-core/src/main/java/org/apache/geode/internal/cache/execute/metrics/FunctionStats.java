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
package org.apache.geode.internal.cache.execute.metrics;

import io.micrometer.core.instrument.MeterRegistry;

import org.apache.geode.Statistics;
import org.apache.geode.annotations.VisibleForTesting;

public interface FunctionStats {

  /**
   * Frees any resources held by this FunctionStats.
   */
  void close();

  /**
   * Returns whether the FunctionStats is closed.
   */
  boolean isClosed();

  /**
   * Returns the current value of the "Total number of completed function.execute() calls" stat.
   *
   * @return the current value of the "function Executions completed" stat
   */
  int getFunctionExecutionsCompleted();

  /**
   * Returns the current value of the "number of currently running invocations" stat.
   *
   * @return the current value of the "functionExecutionsRunning" stat
   */
  int getFunctionExecutionsRunning();

  /**
   * Increments the "ResultsReturnedToResultCollector" stat.
   */
  void incResultsReturned();

  /**
   * Returns the current value of the "Total number of results received and passed to
   * ResultCollector" stat.
   *
   * @return the current value of the "resultsReturned" stat
   */
  int getResultsReceived();

  /**
   * Increments the "ResultsReturnedToResultCollector" stat.
   */
  void incResultsReceived();

  /**
   * Returns the current value of the "Total number of FunctionService...execute() calls" stat.
   *
   * @return the current value of the "functionExecutionsCall" stat
   */
  int getFunctionExecutionCalls();

  /**
   * Increments the "_functionExecutionCallsId" and "_functionExecutionsRunningId" stats and
   * "_functionExecutionHasResultRunningId" in case of function.hasResult = true..
   *
   * @return the current time (ns)
   */
  long startFunctionExecution(boolean haveResult);

  /**
   * Increments the "functionExecutionsCompleted" and "functionExecutionCompleteProcessingTime"
   * stats, as well as updating micrometer timers.
   *
   * @param startTime The start of the functionExecution (which is decremented from the current
   *        time to determine the function Execution processing time).
   * @param haveResult haveResult=true then update the _functionExecutionHasResultRunningId and
   *        _functionExecutionHasResultCompleteProcessingTimeId
   */
  void endFunctionExecution(long startTime, boolean haveResult);

  /**
   * Increments the "_functionExecutionException" and decrements "_functionExecutionsRunningId" and
   * decrement "_functionExecutionHasResultRunningId", as well as updating micrometer timers.
   */
  void endFunctionExecutionWithException(long startTime, boolean haveResult);

  @VisibleForTesting
  Statistics getStatistics();

  @VisibleForTesting
  MeterRegistry getMeterRegistry();
}
