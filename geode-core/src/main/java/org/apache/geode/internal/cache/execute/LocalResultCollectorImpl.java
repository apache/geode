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

public class LocalResultCollectorImpl implements LocalResultCollector {

  private final ResultCollector userRC;

  private CountDownLatch latch = new CountDownLatch(1);

  protected volatile boolean endResultReceived = false;

  private volatile boolean resultCollected = false;

  protected volatile boolean resultsCleared = false;

  private FunctionException functionException = null;

  private Function function = null;

  private AbstractExecution execution = null;

  public LocalResultCollectorImpl(Function function, ResultCollector rc, Execution execution) {
    this.function = function;
    this.userRC = rc;
    this.execution = (AbstractExecution) execution;
  }

  public synchronized void addResult(DistributedMember memberID, Object resultOfSingleExecution) {
    if (resultsCleared) {
      return;
    }
    if (!this.endResultReceived) {
      if (resultOfSingleExecution instanceof Throwable) {
        Throwable t = (Throwable) resultOfSingleExecution;
        if (this.execution.isIgnoreDepartedMembers()) {
          if (t.getCause() != null) {
            t = t.getCause();
          }
          this.userRC.addResult(memberID, t);
        } else {
          if (!(t instanceof InternalFunctionException)) {
            if (this.functionException == null) {
              if (resultOfSingleExecution instanceof FunctionInvocationTargetException) {
                this.functionException = new FunctionException(t);
              } else if (resultOfSingleExecution instanceof FunctionException) {
                this.functionException = (FunctionException) resultOfSingleExecution;
                if (t.getCause() != null) {
                  t = t.getCause();
                }
              } else {
                this.functionException = new FunctionException(t);
              }
            }
            this.functionException.addException(t);
          } else {
            this.userRC.addResult(memberID, t.getCause());
          }
        }
      } else {
        this.userRC.addResult(memberID, resultOfSingleExecution);
      }
    }
  }

  public void endResults() {
    this.endResultReceived = true;
    this.userRC.endResults();
    this.latch.countDown();
  }

  public synchronized void clearResults() {
    this.latch = new CountDownLatch(1);
    this.endResultReceived = false;
    this.functionException = null;
    this.userRC.clearResults();
    resultsCleared = true;
  }

  public Object getResult() throws FunctionException {
    if (this.resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }
    this.resultCollected = true;
    try {
      this.latch.await();
    } catch (InterruptedException e) {
      this.latch.countDown();
      Thread.currentThread().interrupt();
    }
    this.latch = new CountDownLatch(1);
    if (this.functionException != null && !this.execution.isIgnoreDepartedMembers()) {
      if (this.function.isHA()) {
        if (this.functionException
            .getCause() instanceof InternalFunctionInvocationTargetException) {
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.function);
          } else {
            newRc = this.execution.execute(this.function.getId());
          }
          return newRc.getResult();
        }
      }
      throw this.functionException;
    } else {
      Object result = this.userRC.getResult();
      return result;
    }
  }

  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {

    boolean resultReceived = false;
    if (this.resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }
    this.resultCollected = true;
    try {
      resultReceived = this.latch.await(timeout, unit);
    } catch (InterruptedException e) {
      this.latch.countDown();
      Thread.currentThread().interrupt();
    }
    if (!resultReceived) {
      throw new FunctionException(
          "All results not received in time provided");
    }
    this.latch = new CountDownLatch(1);
    if (this.functionException != null && !this.execution.isIgnoreDepartedMembers()) {
      if (this.function.isHA()) {
        if (this.functionException
            .getCause() instanceof InternalFunctionInvocationTargetException) {
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.function);
          } else {
            newRc = this.execution.execute(this.function.getId());
          }
          return newRc.getResult(timeout, unit);
        }
      }
      throw this.functionException;
    } else {
      Object result = this.userRC.getResult(timeout, unit);
      return result;
    }
  }

  public void setException(Throwable exception) {
    if (exception instanceof FunctionException) {
      this.functionException = (FunctionException) exception;
    } else {
      this.functionException = new FunctionException(exception);
    }
  }

  public ReplyProcessor21 getProcessor() {
    // not expected to be invoked
    return null;
  }

  public void setProcessor(ReplyProcessor21 processor) {
    // nothing to be done here since FunctionStreamingResultCollector that
    // wraps this itself implements ReplyProcessor21 and is returned as
    // the ResultCollector so cannot get GCed
  }
}
