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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionInvocationTargetException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.FunctionStreamingReplyMessage;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class FunctionStreamingResultCollector extends ReplyProcessor21
    implements CachedResultCollector {

  private static final Logger logger = LogService.getLogger();

  protected ResultCollector userRC;

  protected Function fn;

  volatile RuntimeException colEx;

  protected boolean resultCollected = false;

  protected final AtomicInteger msgsBeingProcessed = new AtomicInteger();

  private final Map<InternalDistributedMember, Status> statusMap =
      new HashMap<InternalDistributedMember, Status>();

  private Set<InternalDistributedMember> removedNodes = new HashSet<InternalDistributedMember>();

  private volatile boolean finishedWaiting = false;

  private final StreamingFunctionOperation functionResultWaiter;

  private final Object processSingleResult = new Object();

  protected AbstractExecution execution;

  protected volatile boolean endResultReceived = false;

  protected volatile List<FunctionInvocationTargetException> fites;

  private final ResultCollectorHolder rcHolder;

  public FunctionStreamingResultCollector(StreamingFunctionOperation streamingFunctionOperation,
      InternalDistributedSystem system, Set members, ResultCollector rc, Function function,
      AbstractExecution execution) {
    super(system.getDistributionManager(), system, members, null, function.hasResult());
    functionResultWaiter = streamingFunctionOperation;
    userRC = rc;
    fn = function;
    this.execution = execution;
    fites = Collections.synchronizedList(new ArrayList<FunctionInvocationTargetException>());
    // add a reference to self inside the ResultCollector, if required, to avoid
    // this ReplyProcessor21 from being GCed
    if (rc instanceof LocalResultCollector<?, ?>) {
      ((LocalResultCollector<?, ?>) rc).setProcessor(this);
    }
    rcHolder = new ResultCollectorHolder(this);
  }

  @Override
  public void addResult(DistributedMember memId, Object resultOfSingleExecution) {
    if (userRC != null && !endResultReceived) {
      try {
        userRC.addResult(memId, resultOfSingleExecution);
      } catch (RuntimeException badre) {
        colEx = badre;
      } catch (Exception bade) {
        colEx = new RuntimeException(bade);
      }
    }
  }

  @Override
  public void endResults() {
    if (userRC != null) {
      userRC.endResults();
      endResultReceived = true;
    }
  }

  @Override
  public void clearResults() {
    if (userRC != null) {
      endResultReceived = false;
      userRC.clearResults();
    }
    fites.clear();
  }

  @Override
  public Object getResult()
      throws FunctionException {
    return rcHolder.getResult();
  }

  public Object getResultInternal() throws FunctionException {
    if (resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }

    resultCollected = true;
    if (userRC != null) {
      try {
        if (execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) {
          waitForCacheOrFunctionException(0);
        } else {
          waitForRepliesUninterruptibly(0);
        }

        if (removedNodes != null) {
          if (removedNodes.size() != 0) {
            // end the rc and clear it
            clearResults();
            execution = execution.setIsReExecute();
            ResultCollector newRc = null;
            if (execution.isFnSerializationReqd()) {
              newRc = execution.execute(fn);
            } else {
              newRc = execution.execute(fn.getId());
            }
            return newRc.getResult();
          }
        }
        if (!execution.getWaitOnExceptionFlag() && fites.size() > 0) {
          throw new FunctionException(fites.get(0));
        }
      } catch (FunctionInvocationTargetException fite) {
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException iFITE =
              new InternalFunctionInvocationTargetException(fite.getMessage());
          throw new FunctionException(iFITE);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult();
        }
      } catch (CacheClosedException e) {
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException fite =
              new InternalFunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult();
        }
      }
      // catch (CacheException e) {
      // throw new FunctionException(e);
      // }
      catch (ForceReattemptException e) {
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException fite =
              new InternalFunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult();
        }
      } catch (ReplyException e) {
        if (!(execution.waitOnException || execution.forwardExceptions)) {
          throw new FunctionException(e.getCause());
        }
      }
      return userRC.getResult();
    }
    return null;
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return rcHolder.getResult(timeout, unit);
  }

  public Object getResultInternal(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    long timeoutInMillis = unit.toMillis(timeout);
    if (resultCollected) {
      throw new FunctionException(
          "Function results already collected");
    }

    resultCollected = true;
    // Should convert it from unit to milliseconds
    if (userRC != null) {
      try {
        long timeBefore = System.currentTimeMillis();
        boolean isNotTimedOut;
        if (execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) {
          isNotTimedOut = waitForCacheOrFunctionException(timeoutInMillis);
        } else {
          isNotTimedOut = waitForRepliesUninterruptibly(timeoutInMillis);
        }
        if (!isNotTimedOut) {
          throw new FunctionException(
              "All results not received in time provided");
        }
        long timeAfter = System.currentTimeMillis();
        timeoutInMillis = timeoutInMillis - (timeAfter - timeBefore);
        if (timeoutInMillis < 0) {
          timeoutInMillis = 0;
        }

        if (removedNodes != null) {
          if (removedNodes.size() != 0) {
            // end the rc and clear it
            clearResults();
            execution = execution.setIsReExecute();
            ResultCollector newRc = null;
            if (execution.isFnSerializationReqd()) {
              newRc = execution.execute(fn);
            } else {
              newRc = execution.execute(fn.getId());
            }
            return newRc.getResult(timeoutInMillis, unit);
          }
        }
        if (!execution.getWaitOnExceptionFlag() && fites.size() > 0) {
          throw new FunctionException(fites.get(0));
        }
      } catch (FunctionInvocationTargetException fite) { // this is case of WrapperException which
                                                         // enforce the re execution of the
                                                         // function.
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException iFITE =
              new InternalFunctionInvocationTargetException(fite.getMessage());
          throw new FunctionException(iFITE);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      } catch (CacheClosedException e) {
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException fite =
              new InternalFunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      }
      // catch (CacheException e) {
      // endResults();
      // throw new FunctionException(e);
      // }
      catch (ForceReattemptException e) {
        if (!(execution instanceof DistributedRegionFunctionExecutor
            || execution instanceof MultiRegionFunctionExecutor) || !fn.isHA()) {
          FunctionInvocationTargetException fite =
              new FunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else if (execution.isClientServerMode()) {
          clearResults();
          FunctionInvocationTargetException fite =
              new InternalFunctionInvocationTargetException(e.getMessage());
          throw new FunctionException(fite);
        } else {
          clearResults();
          execution = execution.setIsReExecute();
          ResultCollector newRc = null;
          if (execution.isFnSerializationReqd()) {
            newRc = execution.execute(fn);
          } else {
            newRc = execution.execute(fn.getId());
          }
          return newRc.getResult(timeoutInMillis, unit);
        }
      } catch (ReplyException e) {
        if (!(execution.waitOnException || execution.forwardExceptions)) {
          throw new FunctionException(e.getCause());
        }
      }
      return userRC.getResult(timeoutInMillis, unit);
    }
    return null;
  }

  @Override
  protected void postFinish() {
    if (execution.getWaitOnExceptionFlag() && fites.size() > 0) {
      for (int index = 0; index < fites.size(); index++) {
        functionResultWaiter.processData(fites.get(index), true,
            fites.get(index).getMemberId());
      }
    }
  }

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {
    if (id != null) {
      synchronized (members) {
        if (removeMember(id, true)) {
          FunctionInvocationTargetException fe;
          if (execution instanceof DistributedRegionFunctionExecutor
              || execution instanceof MultiRegionFunctionExecutor) {
            if (!fn.isHA()) {
              fe = new FunctionInvocationTargetException(
                  String.format("MemberResponse got memberDeparted event for < %s > crashed, %s",
                      id, Boolean.valueOf(crashed)),
                  id);
            } else {
              fe = new InternalFunctionInvocationTargetException(
                  String.format(
                      "DistributionResponse got memberDeparted event for < %s > crashed, %s",
                      id, Boolean.valueOf(crashed)),
                  id);
              if (execution.isClientServerMode()) {
                if (userRC != null) {
                  endResultReceived = false;
                  userRC.endResults();
                  userRC.clearResults();
                }
              } else {
                if (removedNodes == null) {
                  removedNodes = new HashSet<InternalDistributedMember>();
                }
                removedNodes.add(id);
              }
            }
            fites.add(fe);
          } else {
            fe = new FunctionInvocationTargetException(
                String.format("MemberResponse got memberDeparted event for < %s > crashed, %s",
                    id, Boolean.valueOf(crashed)),
                id);
          }
          fites.add(fe);
        }
      } // synchronized
      checkIfDone();
    }
  }

  /**
   * Waits for the response from the recipient
   *
   * @throws CacheException if the recipient threw a cache exception during message processing
   * @throws ForceReattemptException if the recipient left the distributed system before the
   *         response was received.
   * @throws RegionDestroyedException if the peer has closed its copy of the region
   */
  public boolean waitForCacheOrFunctionException(long timeout)
      throws CacheException, ForceReattemptException {

    boolean timedOut = false;
    try {
      if (timeout == 0) {
        waitForRepliesUninterruptibly();
        timedOut = true;
      } else {
        timedOut = waitForRepliesUninterruptibly(timeout);
      }
    } catch (ReplyException e) {
      removeMember(e.getSender(), true);
      Throwable t = e.getCause();
      if (t instanceof CacheException) {
        throw (CacheException) t;
      } else if (t instanceof RegionDestroyedException) {
        throw (RegionDestroyedException) t;
      } else if (t instanceof ForceReattemptException) {
        logger.info("Peer requests reattempt");
        throw (ForceReattemptException) t;
      } else if (t instanceof PrimaryBucketException) {
        throw new PrimaryBucketException("Peer failed primary test", t);
      }
      if (t instanceof CancelException) {
        execution.failedNodes.add(e.getSender().getId());
        String msg =
            "PartitionResponse got remote CacheClosedException, throwing PartitionedRegionCommunicationException";
        logger.debug("{}, throwing ForceReattemptException", msg, t);
        throw (CancelException) t;
      }
      if (e.getCause() instanceof FunctionException) {
        throw (FunctionException) e.getCause();
      }
      e.handleCause();
    }

    return timedOut;
  }

  protected class Status {
    int msgsProcessed = 0;
    int numMsgs = 0;

    /** Return true if this is the very last reply msg to process for this member */
    protected boolean trackMessage(FunctionStreamingReplyMessage m) {
      msgsProcessed++;

      if (m.isLastMessage()) {
        numMsgs = m.getMessageNumber() + 1;
      }
      return msgsProcessed == numMsgs;
    }
  }

  @Override
  public void process(DistributionMessage msg) {
    if (!waitingOnMember(msg.getSender())) {
      return;
    }
    msgsBeingProcessed.incrementAndGet();
    try {
      ReplyMessage m = (ReplyMessage) msg;
      if (m.getException() == null) {
        FunctionStreamingReplyMessage functionReplyMsg = (FunctionStreamingReplyMessage) m;
        Object result = functionReplyMsg.getResult();
        boolean isLast = false;
        synchronized (processSingleResult) {
          isLast = trackMessage(functionReplyMsg);
          functionResultWaiter.processData(result, isLast, msg.getSender());
        }
        if (isLast) {
          super.process(msg, false); // removes from members and cause us
          // to ignore future messages received from that member
        }
      } else {
        if (execution.forwardExceptions || (execution.waitOnException
        /* && !(m.getException().getCause() instanceof BucketMovedException) */)) {
          // send BucketMovedException forward which will be handled by LocalResultCollectorImpl
          synchronized (processSingleResult) {
            functionResultWaiter.processData(m.getException().getCause(), true,
                msg.getSender());
          }
        }
        super.process(msg, false);
      }
    } finally {
      msgsBeingProcessed.decrementAndGet();
      checkIfDone(); // check to see if decrementing msgsBeingProcessed requires signalling to
                     // proceed
    }
  }

  protected boolean trackMessage(FunctionStreamingReplyMessage m) {
    Status status;
    status = statusMap.get(m.getSender());
    if (status == null) {
      status = new Status();
      statusMap.put(m.getSender(), status);
    }
    return status.trackMessage(m);
  }

  /**
   * Overridden to wait for messages being currently processed: This situation can come about if a
   * member departs while we are still processing data from that member
   */
  @Override
  protected boolean stillWaiting() {
    if (finishedWaiting) { // volatile fetch
      return false;
    }
    if (msgsBeingProcessed.get() > 0 && numMembers() > 0) {
      // to fix bug 37391 always wait for msgsBeingProcessod to go to 0;
      // even if abort is true
      return true;
    }
    // volatile fetches and volatile store:
    finishedWaiting = finishedWaiting || !stillWaitingFromNodes();
    return !finishedWaiting;
  }

  protected boolean stillWaitingFromNodes() {
    if (shutdown) {
      // Create the exception here, so that the call stack reflects the
      // failed computation. If you set the exception in onShutdown,
      // the resulting stack is not of interest.
      ReplyException re = new ReplyException(new DistributedSystemDisconnectedException(
          "aborted due to shutdown"));
      exception = re;
      return false;
    }
    // return (numMembers()-this.numMemberDeparted) > 0;
    return numMembers() > 0;
  }

  @Override
  protected synchronized void processException(ReplyException ex) {
    // we have already forwarded the exception, no need to keep it here
    if (execution.isForwardExceptions() || execution.waitOnException) {
      return;
    }

    // have to keep all the exception
    // rest exception will be added to localresultcollector and it will throw
    // them
    if ((ex.getCause() instanceof CacheClosedException
        || ex.getCause() instanceof ForceReattemptException
        || ex.getCause() instanceof BucketMovedException)) {
      exception = ex;
    } else if (!execution.getWaitOnExceptionFlag()) {
      exception = ex;
    }
  }

  @Override
  protected boolean stopBecauseOfExceptions() {
    if (execution.isIgnoreDepartedMembers()) {
      return false;
    }
    // in case of waitOnException : keep processing
    // the reply from other nodes
    // this exception will be saved in this.exception
    // which will be thrown at the end
    if (execution.waitOnException) {
      return false;
    }
    return super.stopBecauseOfExceptions();
  }
}
