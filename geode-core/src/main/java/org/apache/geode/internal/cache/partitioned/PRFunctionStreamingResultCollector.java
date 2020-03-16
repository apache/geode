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
package org.apache.geode.internal.cache.partitioned;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.CachedResultCollector;
import org.apache.geode.internal.cache.execute.FunctionStreamingResultCollector;
import org.apache.geode.internal.cache.execute.InternalFunctionException;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.LocalResultCollectorImpl;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultWaiter;
import org.apache.geode.internal.cache.execute.ResultCollectorHolder;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PRFunctionStreamingResultCollector extends FunctionStreamingResultCollector
    implements CachedResultCollector {

  private static final Logger logger = LogService.getLogger();

  private boolean hasResult = false;

  private final PartitionedRegionFunctionResultWaiter waiter;

  private final ResultCollectorHolder rcHolder;

  /**
   * Contract of {@link ReplyProcessor21#stillWaiting()} is that it never returns true after
   * returning false.
   */

  public PRFunctionStreamingResultCollector(
      PartitionedRegionFunctionResultWaiter partitionedRegionFunctionResultWaiter,
      InternalDistributedSystem system, Set<InternalDistributedMember> members, ResultCollector rc,
      Function functionObject, PartitionedRegion pr, AbstractExecution execution) {
    super(partitionedRegionFunctionResultWaiter, system, members, rc, functionObject, execution);
    this.waiter = partitionedRegionFunctionResultWaiter;
    this.hasResult = functionObject.hasResult();
    rcHolder = new ResultCollectorHolder(this);
  }

  @Override
  public void addResult(DistributedMember memId, Object resultOfSingleExecution) {
    if (!this.endResultReceived) {
      if (!(this.userRC instanceof LocalResultCollectorImpl)
          && resultOfSingleExecution instanceof InternalFunctionException) {
        resultOfSingleExecution = ((InternalFunctionException) resultOfSingleExecution).getCause();
      }
      this.userRC.addResult(memId, resultOfSingleExecution);
    }
  }

  @Override
  public Object getResult() throws FunctionException {
    return rcHolder.getResult();
  }

  @Override
  public Object getResultInternal() throws FunctionException {
    if (this.resultCollected) {
      throw new FunctionException("Result already collected");
    }

    this.resultCollected = true;
    if (this.hasResult) {
      try {
        this.waitForCacheOrFunctionException(0);
        if (!this.execution.getFailedNodes().isEmpty() && !this.execution.isClientServerMode()) {
          // end the rc and clear it
          endResults();
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult();
        }
        if (!this.execution.getWaitOnExceptionFlag() && this.fites.size() > 0) {
          throw new FunctionException(this.fites.get(0));
        }
      } catch (FunctionInvocationTargetException fite) {
        // this is case of WrapperException which enforce the re execution of
        // the function.
        if (!execution.getWaitOnExceptionFlag()) {
          if (!this.fn.isHA()) {
            throw new FunctionException(fite);
          } else if (execution.isClientServerMode()) {
            clearResults();
            FunctionInvocationTargetException iFITE = new InternalFunctionInvocationTargetException(
                fite.getMessage(), this.execution.getFailedNodes());
            throw new FunctionException(iFITE);
          } else {
            clearResults();
            this.execution = this.execution.setIsReExecute();
            ResultCollector newRc = null;
            if (execution.isFnSerializationReqd()) {
              newRc = this.execution.execute(this.fn);
            } else {
              newRc = this.execution.execute(this.fn.getId());
            }
            return newRc.getResult();
          }
        }
      } catch (BucketMovedException e) {
        if (!execution.getWaitOnExceptionFlag()) {
          if (!this.fn.isHA()) {
            // endResults();
            FunctionInvocationTargetException fite =
                new FunctionInvocationTargetException(e.getMessage());
            throw new FunctionException(fite);
          } else if (execution.isClientServerMode()) {
            // endResults();
            clearResults();
            FunctionInvocationTargetException fite =
                new InternalFunctionInvocationTargetException(e.getMessage());
            throw new FunctionException(fite);
          } else {
            // endResults();
            clearResults();
            this.execution = this.execution.setIsReExecute();
            ResultCollector newRc = null;
            if (execution.isFnSerializationReqd()) {
              newRc = this.execution.execute(this.fn);
            } else {
              newRc = this.execution.execute(this.fn.getId());
            }
            return newRc.getResult();
          }
        }
      } catch (CacheClosedException e) {
        if (!execution.getWaitOnExceptionFlag()) {
          if (!this.fn.isHA()) {
            // endResults();
            FunctionInvocationTargetException fite =
                new FunctionInvocationTargetException(e.getMessage());
            throw new FunctionException(fite);
          } else if (execution.isClientServerMode()) {
            // endResults();
            clearResults();
            FunctionInvocationTargetException fite = new InternalFunctionInvocationTargetException(
                e.getMessage(), this.execution.getFailedNodes());
            throw new FunctionException(fite);
          } else {
            // endResults();
            clearResults();
            this.execution = this.execution.setIsReExecute();
            ResultCollector newRc = null;
            if (execution.isFnSerializationReqd()) {
              newRc = this.execution.execute(this.fn);
            } else {
              newRc = this.execution.execute(this.fn.getId());
            }
            return newRc.getResult();
          }
        }
      } catch (CacheException e) {
        // endResults();
        throw new FunctionException(e);
      } catch (ForceReattemptException e) {

        // this is case of WrapperException which enforce the re execution of
        // the function.
        if (!this.fn.isHA()) {
          throw new FunctionException(e);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException iFITE = new InternalFunctionInvocationTargetException(
              e.getMessage(), this.execution.getFailedNodes());
          throw new FunctionException(iFITE);
        } else {
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult();
        }
      }
    }
    return this.userRC.getResult();
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return rcHolder.getResult(timeout, unit);
  }

  @Override
  public Object getResultInternal(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    long timeoutInMillis = unit.toMillis(timeout);
    if (this.resultCollected) {
      throw new FunctionException("Result already collected");
    }
    this.resultCollected = true;
    if (this.hasResult) {
      try {
        long timeBefore = System.currentTimeMillis();
        if (!this.waitForCacheOrFunctionException(timeoutInMillis)) {
          throw new FunctionException("All results not received in time provided.");
        }
        long timeAfter = System.currentTimeMillis();
        timeoutInMillis = timeoutInMillis - (timeAfter - timeBefore);
        if (timeoutInMillis < 0) {
          timeoutInMillis = 0;
        }

        if (!this.execution.getFailedNodes().isEmpty() && !this.execution.isClientServerMode()) {
          // end the rc and clear it
          endResults();
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
        if (!this.execution.getWaitOnExceptionFlag() && this.fites.size() > 0) {
          throw new FunctionException(this.fites.get(0));
        }
      } catch (FunctionInvocationTargetException fite) {
        if (!this.fn.isHA()) {
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException fe = new InternalFunctionInvocationTargetException(
              fite.getMessage(), this.execution.getFailedNodes());
          throw new FunctionException(fe);
        } else {
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      } catch (BucketMovedException e) {
        if (!this.fn.isHA()) {
          // endResults();
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          // endResults();
          clearResults();
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else {
          // endResults();
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      } catch (CacheClosedException e) {
        if (!this.fn.isHA()) {
          // endResults();
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          // endResults();
          clearResults();
          FunctionInvocationTargetException fite = new InternalFunctionInvocationTargetException(
              e.getMessage(), this.execution.getFailedNodes());
          throw new FunctionException(fite);
        } else {
          // endResults();
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      } catch (CacheException e) {
        // endResults();
        throw new FunctionException(e);
      } catch (ForceReattemptException e) {
        // this is case of WrapperException which enforce the re execution of
        // the function.
        if (!this.fn.isHA()) {
          throw new FunctionException(e);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException iFITE = new InternalFunctionInvocationTargetException(
              e.getMessage(), this.execution.getFailedNodes());
          throw new FunctionException(iFITE);
        } else {
          clearResults();
          this.execution = this.execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = this.execution.execute(this.fn);
          } else {
            newRc = this.execution.execute(this.fn.getId());
          }
          return newRc.getResult();
        }
      }
    }
    return this.userRC.getResult(timeoutInMillis, unit); // As we have already waited for timeout
                                                         // earlier we expect results to be ready
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {
    FunctionInvocationTargetException fite;
    if (id != null) {
      synchronized (this.members) {
        if (removeMember(id, true)) {
          if (!this.fn.isHA()) {
            fite = new FunctionInvocationTargetException(
                String.format("memberDeparted event for < %s > crashed, %s",
                    new Object[] {id, Boolean.valueOf(crashed)}),
                id);
          } else {
            fite = new InternalFunctionInvocationTargetException(
                String.format("memberDeparted event for < %s > crashed, %s",
                    new Object[] {id, Boolean.valueOf(crashed)}),
                id);
            this.execution.addFailedNode(id.getId());
          }
          this.fites.add(fite);
        }
        checkIfDone();
      }
    } else {
      Exception e = new Exception(
          "memberDeparted got null memberId");
      logger.info(String.format("memberDeparted got null memberId crashed=%s",
          Boolean.valueOf(crashed)),
          e);
    }
  }



  @Override
  protected synchronized void processException(DistributionMessage msg, ReplyException ex) {
    logger.debug(
        "StreamingPartitionResponseWithResultCollector received exception {} from member {}",
        ex.getCause(), msg.getSender());

    // we have already forwarded the exception, no need to keep it here
    if (execution.isForwardExceptions() || execution.getWaitOnExceptionFlag()) {
      return;
    }

    /*
     * Below two cases should also be handled and not thrown exception Saving the exception
     * ForeceReattempt can also be added here? Also, if multipel nodes throw exception, one may
     * override another TODO: Wrap exception among each other or create a list of exceptions like
     * this.fite.
     */
    if (ex.getCause() instanceof CacheClosedException) {
      ((PartitionedRegionFunctionExecutor) this.execution).addFailedNode(msg.getSender().getId());
      this.exception = ex;
    } else if (ex.getCause() instanceof BucketMovedException) {
      this.exception = ex;
    } else if (!execution.getWaitOnExceptionFlag()) {
      this.exception = ex;
    }
  }
}
