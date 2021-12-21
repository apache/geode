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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ReplyProcessor21;

public class LocalResultCollectorImpl implements CachedResultCollector, LocalResultCollector {

  private final ResultCollector userRC;

  private CountDownLatch latch = new CountDownLatch(1);

  protected volatile boolean endResultReceived = false;

  private volatile boolean resultCollected = false;

  protected volatile boolean resultsCleared = false;

  private FunctionException functionException = null;

  private Function function = null;

  private AbstractExecution execution = null;

  private final ResultCollectorHolder rcHolder;

  public LocalResultCollectorImpl(Function function, ResultCollector rc, Execution execution) {
    this.function = function;
    userRC = rc;
    this.execution = (AbstractExecution) execution;
    rcHolder = new ResultCollectorHolder(this);
  }

  @Override
  public synchronized void addResult(DistributedMember memberID, Object resultOfSingleExecution) {
    if (resultsCleared) {
      return;
    }
    if (!endResultReceived) {
      if (resultOfSingleExecution instanceof Throwable) {
        Throwable t = (Throwable) resultOfSingleExecution;
        if (execution.isIgnoreDepartedMembers()) {
          if (t.getCause() != null) {
            t = t.getCause();
          }
          userRC.addResult(memberID, t);
        } else {
          if (!(t instanceof InternalFunctionException)) {
            if (functionException == null) {
              if (resultOfSingleExecution instanceof FunctionInvocationTargetException) {
                functionException = new FunctionException(t);
              } else if (resultOfSingleExecution instanceof FunctionException) {
                functionException = (FunctionException) resultOfSingleExecution;
                if (t.getCause() != null) {
                  t = t.getCause();
                }
              } else {
                functionException = new FunctionException(t);
              }
            }
            functionException.addException(t);
          } else {
            userRC.addResult(memberID, t.getCause());
          }
        }
      } else {
        userRC.addResult(memberID, resultOfSingleExecution);
      }
    }
  }

  @Override
  public void endResults() {
    endResultReceived = true;
    userRC.endResults();
    latch.countDown();
  }

  @Override
  public synchronized void clearResults() {
    latch = new CountDownLatch(1);
    endResultReceived = false;
    functionException = null;
    userRC.clearResults();
    resultsCleared = true;
  }

  @Override
  public Object getResult()
      throws FunctionException {
    return rcHolder.getResult();
  }

  @Override
  public Object getResultInternal() throws FunctionException {
    if (resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }
    resultCollected = true;
    try {
      latch.await();
    } catch (InterruptedException e) {
      latch.countDown();
      Thread.currentThread().interrupt();
    }
    latch = new CountDownLatch(1);
    if (functionException != null && !execution.isIgnoreDepartedMembers()) {
      if (function.isHA()) {
        if (functionException
            .getCause() instanceof InternalFunctionInvocationTargetException) {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(function);
          } else {
            newRc = execution.execute(function.getId());
          }
          return newRc.getResult();
        }
      }
      throw functionException;
    } else {
      Object result = userRC.getResult();
      return result;
    }
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return rcHolder.getResult(timeout, unit);
  }

  @Override
  public Object getResultInternal(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {

    boolean resultReceived = false;
    if (resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }
    resultCollected = true;
    try {
      resultReceived = latch.await(timeout, unit);
    } catch (InterruptedException e) {
      latch.countDown();
      Thread.currentThread().interrupt();
    }
    if (!resultReceived) {
      throw new FunctionException(
          "All results not received in time provided");
    }
    latch = new CountDownLatch(1);
    if (functionException != null && !execution.isIgnoreDepartedMembers()) {
      if (function.isHA()) {
        if (functionException
            .getCause() instanceof InternalFunctionInvocationTargetException) {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(function);
          } else {
            newRc = execution.execute(function.getId());
          }
          return newRc.getResult(timeout, unit);
        }
      }
      throw functionException;
    } else {
      Object result = userRC.getResult(timeout, unit);
      return result;
    }
  }

  @Override
  public void setException(Throwable exception) {
    if (exception instanceof FunctionException) {
      functionException = (FunctionException) exception;
    } else {
      functionException = new FunctionException(exception);
    }
  }

  @Override
  public ReplyProcessor21 getProcessor() {
    // not expected to be invoked
    return null;
  }

  @Override
  public void setProcessor(ReplyProcessor21 processor) {
    // nothing to be done here since FunctionStreamingResultCollector that
    // wraps this itself implements ReplyProcessor21 and is returned as
    // the ResultCollector so cannot get GCed
  }
}
